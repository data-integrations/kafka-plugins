/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.format.RecordFormats;
import co.cask.hydrator.common.KeyValueListParser;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import kafka.common.TopicAndPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Kafka batch source.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(KafkaBatchSource.NAME)
@Description("Kafka batch source.")
public class KafkaBatchSource extends BatchSource<KafkaKey, KafkaMessage, StructuredRecord> {
  public static final String NAME = "Kafka";

  private final KafkaBatchConfig config;
  private KeyValueTable table;
  private List<KafkaRequest> kafkaRequests;
  private Schema schema;
  private RecordFormat<StreamEvent, StructuredRecord> recordFormat;
  private String messageField;

  /**
   * Config properties for the plugin.
   */
  public static class KafkaBatchConfig extends ReferencePluginConfig {

    @Description("Kafka topic to read from.")
    @Macro
    private String topic;

    @Description("List of Kafka brokers specified in host1:port1,host2:port2 form. For example, " +
      "host1.example.com:9092,host2.example.com:9092.")
    @Macro
    private String kafkaBrokers;

    @Description("Table name to track the latest offset we read from kafka. It is recommended to name it " +
      "same as the pipeline name to avoid conflict on table names.")
    @Macro
    @Nullable
    private String tableName;

    @Description("A comma separated list of topic partitions to read from. " +
      "If not specified, all partitions will be read.")
    @Macro
    @Nullable
    private String partitions;

    @Description("The initial offset for each topic partition in partition1:offset1,partition2:offset2 form. " +
      "These offsets will only be used for the first run of the pipeline. " +
      "Any subsequent run will read from the latest offset from previous run." +
      "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. " +
      "If not specified, the initial run will start reading from the latest message in Kafka.")
    @Nullable
    @Macro
    private String initialPartitionOffsets;

    @Description("Output schema of the source, including the timeField and keyField. " +
      "The fields excluding keyField are used in conjunction with the format " +
      "to parse Kafka payloads.")
    private String schema;

    @Description("Optional format of the Kafka event. Any format supported by CDAP is supported. " +
      "For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values. " +
      "If no format is given, Kafka message payloads will be treated as bytes.")
    @Nullable
    private String format;

    @Description("Optional name of the field containing the message key. " +
      "If this is not set, no key field will be added to output records. " +
      "If set, this field must be present in the schema property and must be bytes.")
    @Nullable
    private String keyField;

    @Description("Optional name of the field containing the kafka partition that was read from. " +
      "If this is not set, no partition field will be added to output records. " +
      "If set, this field must be present in the schema property and must be an integer.")
    @Nullable
    private String partitionField;

    @Description("Optional name of the field containing the kafka offset that the message was read from. " +
      "If this is not set, no offset field will be added to output records. " +
      "If set, this field must be present in the schema property and must be a long.")
    @Nullable
    private String offsetField;

    public KafkaBatchConfig() {
      super("");
    }

    public KafkaBatchConfig(String brokers, String partitions, String topic, String initialPartitionOffsets,
                            Long defaultOffset) {
      super(String.format("Kafka_%s", topic));
      this.kafkaBrokers = brokers;
      this.partitions = partitions;
      this.topic = topic;
      this.initialPartitionOffsets = initialPartitionOffsets;
    }

    // Accessors
    public String getTopic() {
      return topic;
    }

    public String getBrokers() {
      return kafkaBrokers;
    }

    public String getTableName() {
      return tableName;
    }

    public Set<Integer> getPartitions() {
      Set<Integer> partitionSet = new HashSet<>();
      if (partitions == null) {
        return partitionSet;
      }
      for (String partition : Splitter.on(',').trimResults().split(partitions)) {
        try {
          partitionSet.add(Integer.parseInt(partition));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
            String.format("Invalid partition '%s'. Partitions must be integers.", partition));
        }
      }
      return partitionSet;
    }

    @Nullable
    public String getKeyField() {
      return Strings.isNullOrEmpty(keyField) ? null : keyField;
    }

    @Nullable
    public String getPartitionField() {
      return Strings.isNullOrEmpty(partitionField) ? null : partitionField;
    }

    @Nullable
    public String getOffsetField() {
      return Strings.isNullOrEmpty(offsetField) ? null : offsetField;
    }

    @Nullable
    public String getFormat() {
      return Strings.isNullOrEmpty(format) ? null : format;
    }

    public Schema getSchema() {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
      }
    }

    /**
     * Gets the message schema from the schema field. If the time, key, partition,
     * or offset fields are in the configured schema, they will be removed.
     */
    public Schema getMessageSchema() {
      Schema schema = getSchema();
      List<Schema.Field> messageFields = new ArrayList<>();
      boolean keyFieldExists = false;
      boolean partitionFieldExists = false;
      boolean offsetFieldExists = false;

      for (Schema.Field field : schema.getFields()) {
        String fieldName = field.getName();
        Schema fieldSchema = field.getSchema();
        Schema.Type fieldType = fieldSchema.isNullable()
          ? fieldSchema.getNonNullable().getType()
          : fieldSchema.getType();
        // if the field is not the time field and not the key field, it is a message field.
        if (fieldName.equals(keyField)) {
          if (fieldType != Schema.Type.BYTES) {
            throw new IllegalArgumentException("The key field must be of type bytes or nullable bytes.");
          }
          keyFieldExists = true;
        } else if (fieldName.equals(partitionField)) {
          if (fieldType != Schema.Type.INT) {
            throw new IllegalArgumentException("The partition field must be of type int.");
          }
          partitionFieldExists = true;
        } else if (fieldName.equals(offsetField)) {
          if (fieldType != Schema.Type.LONG) {
            throw new IllegalArgumentException("The offset field must be of type long.");
          }
          offsetFieldExists = true;
        } else {
          messageFields.add(field);
        }
      }
      if (messageFields.isEmpty()) {
        throw new IllegalArgumentException(
          "Schema must contain at least one other field besides the time and key fields.");
      }
      if (getKeyField() != null && !keyFieldExists) {
        throw new IllegalArgumentException(String.format(
          "keyField '%s' does not exist in the schema. Please add it to the schema.", keyField));
      }
      if (getPartitionField() != null && !partitionFieldExists) {
        throw new IllegalArgumentException(String.format(
          "partitionField '%s' does not exist in the schema. Please add it to the schema.", partitionField));
      }
      if (getOffsetField() != null && !offsetFieldExists) {
        throw new IllegalArgumentException(String.format(
          "offsetField '%s' does not exist in the schema. Please add it to the schema.", offsetField));
      }
      return Schema.recordOf("kafka.message", messageFields);
    }

    /**
     * @return broker host to broker port mapping.
     */
    public Map<String, Integer> getBrokerMap() {
      Map<String, Integer> brokerMap = new HashMap<>();
      for (KeyValue<String, String> hostAndPort : KeyValueListParser.DEFAULT.parse(kafkaBrokers)) {
        String host = hostAndPort.getKey();
        String portStr = hostAndPort.getValue();
        try {
          brokerMap.put(host, Integer.parseInt(portStr));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(String.format(
            "Invalid port '%s' for host '%s'.", portStr, host));
        }
      }
      if (brokerMap.isEmpty()) {
        throw new IllegalArgumentException("Must specify kafka brokers.");
      }
      return brokerMap;
    }

    public Map<TopicAndPartition, Long> getInitialPartitionOffsets() {
      Map<TopicAndPartition, Long> partitionOffsets = new HashMap<>();

      // if initial partition offsets are specified, overwrite the defaults.
      if (initialPartitionOffsets != null) {
        for (KeyValue<String, String> partitionAndOffset : KeyValueListParser.DEFAULT.parse(initialPartitionOffsets)) {
          String partitionStr = partitionAndOffset.getKey();
          String offsetStr = partitionAndOffset.getValue();
          int partition;
          try {
            partition = Integer.parseInt(partitionStr);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(
              "Invalid partition '%s' in initialPartitionOffsets.", partitionStr));
          }
          long offset;
          try {
            offset = Long.parseLong(offsetStr);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(
              "Invalid offset '%s' in initialPartitionOffsets for partition %d.", partitionStr, partition));
          }
          partitionOffsets.put(new TopicAndPartition(topic, partition), offset);
        }
      }

      return partitionOffsets;
    }

    public void validate() {
      // brokers can be null since it is macro enabled.
      if (kafkaBrokers != null) {
        getBrokerMap();
      }
      getPartitions();
      getInitialPartitionOffsets();

      Schema messageSchema = getMessageSchema();
      // if format is empty, there must be just a single message field of type bytes or nullable types.
      if (Strings.isNullOrEmpty(format)) {
        List<Schema.Field> messageFields = messageSchema.getFields();
        if (messageFields.size() > 1) {
          String fieldNames = messageFields.stream().map(Schema.Field::getName).collect(Collectors.joining(","));
          throw new IllegalArgumentException(String.format(
            "Without a format, the schema must contain just a single message field of type bytes or nullable bytes. " +
              "Found %s message fields (%s).", messageFields.size(), fieldNames));
        }

        Schema.Field messageField = messageFields.get(0);
        Schema messageFieldSchema = messageField.getSchema();
        Schema.Type messageFieldType = messageFieldSchema.isNullable() ?
          messageFieldSchema.getNonNullable().getType() : messageFieldSchema.getType();
        if (messageFieldType != Schema.Type.BYTES) {
          throw new IllegalArgumentException(String.format(
            "Without a format, the message field must be of type bytes or nullable bytes, but field %s is of type %s.",
            messageField.getName(), messageField.getSchema()));
        }
      } else {
        // otherwise, if there is a format, make sure we can instantiate it.
        FormatSpecification formatSpec = new FormatSpecification(format, messageSchema, new HashMap<>());

        try {
          RecordFormats.createInitializedFormat(formatSpec);
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format(
            "Unable to instantiate a message parser from format '%s' and message schema '%s': %s",
            format, messageSchema, e.getMessage()), e);
        }
      }
    }
  }

  public KafkaBatchSource(KafkaBatchConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    if (config.getTableName() != null) {
      pipelineConfigurer.createDataset(config.getTableName(), KeyValueTable.class, DatasetProperties.EMPTY);
    }
    config.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();
    String tableName = config.getTableName() != null ? config.getTableName() : config.getTopic();
    if (!context.datasetExists(tableName)) {
      context.createDataset(tableName, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
    }
    table = context.getDataset(tableName);
    kafkaRequests = KafkaInputFormat.saveKafkaRequests(conf, config.getTopic(), config.getBrokerMap(),
                                                       config.getPartitions(), config.getInitialPartitionOffsets(),
                                                       table);
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    Schema schema = config.getSchema();
    if (schema != null) {
      lineageRecorder.createExternalDataset(schema);
      if (schema.getFields() != null && !schema.getFields().isEmpty()) {
        lineageRecorder.recordRead("Read", "Read from Kafka topic.",
                                   schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
      }
    }
    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(KafkaInputFormat.class, conf)));
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    if (succeeded) {
      for (KafkaRequest kafkaRequest : kafkaRequests) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(kafkaRequest.getTopic(),
                                                                    kafkaRequest.getPartition());
        table.write(topicAndPartition.toString(), Bytes.toBytes(kafkaRequest.getLastOffset()));
      }
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    schema = config.getSchema();
    Schema messageSchema = config.getMessageSchema();
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      if (!name.equals(config.getKeyField()) && !name.equals(config.getPartitionField()) &&
        !name.equals(config.getOffsetField())) {
        messageField = name;
        break;
      }
    }
    if (config.getFormat() != null) {
      FormatSpecification spec =
        new FormatSpecification(config.getFormat(), messageSchema, new HashMap<>());
      recordFormat = RecordFormats.createInitializedFormat(spec);
    }
  }

  @Override
  public void transform(KeyValue<KafkaKey, KafkaMessage> input, Emitter<StructuredRecord> emitter) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (config.getKeyField() != null) {
      builder.set(config.getKeyField(), input.getValue().getKey().array());
    }
    if (config.getPartitionField() != null) {
      builder.set(config.getPartitionField(), input.getKey().getPartition());
    }
    if (config.getOffsetField() != null) {
      builder.set(config.getOffsetField(), input.getKey().getOffset());
    }
    if (config.getFormat() == null) {
      builder.set(messageField, ByteBuffers.getByteArray(input.getValue().getPayload()));
    } else {
      StructuredRecord messageRecord = recordFormat.read(new StreamEvent(input.getValue().getPayload()));
      for (Schema.Field field : messageRecord.getSchema().getFields()) {
        String fieldName = field.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
    }
    emitter.emit(builder.build());
  }
}
