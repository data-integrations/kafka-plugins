/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.confluent.streaming.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;
import java.util.Objects;

/**
 * Confluent Kafka Streaming sink.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(ConfluentStreamingSink.PLUGIN_NAME)
@Description("Confluent Kafka streaming sink.")
public class ConfluentStreamingSink extends SparkSink<StructuredRecord> {
  public static final String PLUGIN_NAME = "Confluent";

  private final ConfluentStreamingSinkConfig conf;

  public ConfluentStreamingSink(ConfluentStreamingSinkConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    conf.validate(inputSchema, failureCollector);
    failureCollector.getOrThrowException();
  }

  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) {
    Schema inputSchema = sparkPluginContext.getInputSchema();
    FailureCollector failureCollector = sparkPluginContext.getFailureCollector();
    conf.validate(inputSchema, failureCollector);
    failureCollector.getOrThrowException();
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) {
    Map<String, Object> producerParams = ConfluentStreamingSinkUtil.getProducerParams(conf, context.getPipelineName());
    Schema inputSchema = Objects.requireNonNull(context.getInputSchema());
    Schema outputSchema = conf.getMessageSchema(inputSchema);
    StructuredToProducerRecordTransformer transformer = new StructuredToProducerRecordTransformer(conf, outputSchema);
    javaRDD.foreachPartition(structuredRecordIterator -> {
      try (Producer<Object, Object> producer = new KafkaProducer<>(producerParams)) {
        while (structuredRecordIterator.hasNext()) {
          StructuredRecord input = structuredRecordIterator.next();
          ProducerRecord<Object, Object> record = transformer.transform(input);
          producer.send(record);
          if (!conf.getAsync()) {
            producer.flush();
          }
        }
      }
    });
  }
}
