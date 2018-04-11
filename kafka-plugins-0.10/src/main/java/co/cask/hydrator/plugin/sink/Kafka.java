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

package co.cask.hydrator.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.KeyValueListParser;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.plugin.common.KafkaHelpers;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Kafka sink to write to Kafka
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("Kafka")
@Description("KafkaSink to write events to kafka")
public class Kafka extends ReferenceBatchSink<StructuredRecord, Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(Kafka.class);

  // Configuration for the plugin.
  private final Config producerConfig;

  private final KafkaOutputFormatProvider kafkaOutputFormatProvider;

  // Static constants for configuring Kafka producer.
  private static final String BROKER_LIST = "bootstrap.servers";
  private static final String ACKS_REQUIRED = "acks";

  public Kafka(Config producerConfig) {
    super(producerConfig);
    this.producerConfig = producerConfig;
    this.kafkaOutputFormatProvider = new KafkaOutputFormatProvider(producerConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    if (!producerConfig.async.equalsIgnoreCase("true") && !producerConfig.async.equalsIgnoreCase("false")) {
      throw new IllegalArgumentException("Async flag has to be either TRUE or FALSE.");
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.of(producerConfig.referenceName, kafkaOutputFormatProvider));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Text, Text>> emitter)
    throws Exception {
    List<Schema.Field> fields = input.getSchema().getFields();

      String body;
      if (producerConfig.format.equalsIgnoreCase("json")) {
        body = StructuredRecordStringConverter.toJsonString(input);
      } else {
        List<Object> objs = getExtractedValues(input, fields);
        body = StringUtils.join(objs, ",");
      }

      if (Strings.isNullOrEmpty(producerConfig.key)) {
        emitter.emit(new KeyValue<>((Text) null, new Text(body)));
      } else {
        String key = input.get(producerConfig.key);
        emitter.emit(new KeyValue<>(new Text(key), new Text(body)));
      }
  }

  private List<Object> getExtractedValues(StructuredRecord input, List<Schema.Field> fields) {
    // Extract all values from the structured record
    List<Object> objs = Lists.newArrayList();
    for (Schema.Field field : fields) {
      objs.add(input.get(field.getName()));
    }
    return objs;
  }


  @Override
  public void destroy() {
    super.destroy();
  }

  /**
   * Kafka Producer Configuration.
   */
  public static class Config extends ReferencePluginConfig {

    @Name("brokers")
    @Description("Specifies the connection string where Producer can find one or more brokers to " +
      "determine the leader for each topic")
    @Macro
    private String brokers;

    @Name("async")
    @Description("Specifies whether an acknowledgment is required from broker that message was received. " +
      "Default is FALSE")
    @Macro
    private String async;

    @Name("key")
    @Description("Specify the key field to be used in the message. Only String Partitioner is supported.")
    @Macro
    @Nullable
    private String key;

    @Name("topic")
    @Description("Topic to which message needs to be published")
    @Macro
    private String topic;

    @Name("format")
    @Description("Format a structured record should be converted to")
    @Macro
    private String format;

    @Name("kafkaProperties")
    @Description("Additional kafka producer properties to set")
    @Macro
    @Nullable
    private String kafkaProperties;

    @Description("The kerberos principal used for the source.")
    @Macro
    @Nullable
    private String principal;

    @Description("The keytab location for the kerberos principal when kerberos security is enabled for kafka.")
    @Macro
    @Nullable
    private String keytabLocation;

    @Name("compressionType")
    @Description("Additional kafka producer properties to set")
    @Macro
    private String compressionType;

    public Config(String brokers, String async, String key, String topic, String format, String kafkaProperties,
                  String compressionType) {
      super(String.format("Kafka_%s", topic));
      this.brokers = brokers;
      this.async = async;
      this.key = key;
      this.topic = topic;
      this.format = format;
      this.kafkaProperties = kafkaProperties;
      this.compressionType = compressionType;
    }
  }

  private static class KafkaOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    KafkaOutputFormatProvider(Config kafkaSinkConfig) {
      this.conf = new HashMap<>();
      conf.put("topic", kafkaSinkConfig.topic);

      conf.put(BROKER_LIST, kafkaSinkConfig.brokers);
      conf.put("compression.type", kafkaSinkConfig.compressionType);

      KafkaHelpers.setupKerberosLogin(conf, kafkaSinkConfig.principal, kafkaSinkConfig.keytabLocation);
      addKafkaProperties(kafkaSinkConfig.kafkaProperties);

      conf.put("async", kafkaSinkConfig.async);
      if (kafkaSinkConfig.async.equalsIgnoreCase("true")) {
        conf.put(ACKS_REQUIRED, "1");
      }

      if (!Strings.isNullOrEmpty(kafkaSinkConfig.key)) {
        conf.put("hasKey", kafkaSinkConfig.key);
      }
    }

    private void addKafkaProperties(String kafkaProperties) {
      KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
      if (!Strings.isNullOrEmpty(kafkaProperties)) {
        for (KeyValue<String, String> keyVal : kvParser.parse(kafkaProperties)) {
          // add prefix to each property
          String key = "additional." + keyVal.getKey();
          String val = keyVal.getValue();
          conf.put(key, val);
        }
      }
    }

    @Override
    public String getOutputFormatClassName() {
      return KafkaOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
