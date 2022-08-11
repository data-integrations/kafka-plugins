/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.sink;

import com.google.common.base.Strings;
import io.cdap.plugin.common.KafkaHelpers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Output format to write to kafka
 */
public class KafkaOutputFormat extends OutputFormat<Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOutputFormat.class);
  // default value found with experiments
  private static final int DEFAULT_LINGER_MS_CONFIG = 100;

  private KafkaProducer<String, String> producer;

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) {
        // no-op
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) {
        //no-op
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) {
        //no-op
      }

      @Override
      public void abortTask(TaskAttemptContext taskContext) {
        //no-op
      }
    };
  }

  @Override
  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) {
    Configuration configuration = context.getConfiguration();

    // Extract the topics
    String topic = configuration.get("topic");

    // CDAP-9178: cached the producer object to avoid being created on every batch interval
    if (producer == null) {
      producer = initializeProducer(configuration);
    }

    return new KafkaRecordWriter(producer, topic);
  }

  private KafkaProducer<String, String> initializeProducer(Configuration configuration) {
    Properties props = new Properties();
    // Configure the properties for kafka.
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            configuration.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            configuration.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            configuration.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    props.put("compression.type", configuration.get("compression.type"));


    if (!Strings.isNullOrEmpty(configuration.get("hasKey"))) {
      // set partitioner class only if key is provided
      props.put("partitioner.class", "io.cdap.plugin.sink.StringPartitioner");
    }

    if (configuration.get("async") != null && configuration.get("async").equalsIgnoreCase("true")) {
      props.put("producer.type", "async");
      props.put(ProducerConfig.ACKS_CONFIG, "1");
    } else {
      props.put("producer.type", "sync");
    }

    Map<String, String> valByRegex = configuration.getValByRegex("^additional.");

    for (Map.Entry<String, String> entry : valByRegex.entrySet()) {
      //strip off the prefix we added while creating the conf.
      props.put(entry.getKey().substring(11), entry.getValue());
      LOG.info("Property key: {}, value: {}", entry.getKey().substring(11), entry.getValue());
    }

    // Add Kerberos login information if any
    if (!Strings.isNullOrEmpty(configuration.get(KafkaHelpers.SASL_JAAS_CONFIG))) {
      props.put(KafkaHelpers.SASL_JAAS_CONFIG, configuration.get(KafkaHelpers.SASL_JAAS_CONFIG));
    }

    // Set linger.ms property to a default value if not specified by user.
    props.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, DEFAULT_LINGER_MS_CONFIG);
    return new KafkaProducer<>(props);
  }
}

