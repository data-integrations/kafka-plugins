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

package io.cdap.plugin.confluent.streaming.source;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.confluent.common.ConfigValidations;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Conf for Confluent Kafka streaming source.
 */
@SuppressWarnings("unused")
public class ConfluentStreamingSourceConfig extends ReferencePluginConfig implements Serializable {
  public static final String NAME_SCHEMA = "schema";
  public static final String NAME_BROKERS = "brokers";
  public static final String NAME_TOPIC = "topic";
  public static final String NAME_PARTITIONS = "partitions";
  public static final String NAME_MAX_RATE = "maxRatePerPartition";
  public static final String NAME_INITIAL_PARTITION_OFFSETS = "initialPartitionOffsets";
  public static final String NAME_DEFAULT_INITIAL_OFFSET = "defaultInitialOffset";
  public static final String NAME_TIMEFIELD = "timeField";
  public static final String NAME_KEYFIELD = "keyField";
  public static final String NAME_PARTITION_FIELD = "partitionField";
  public static final String NAME_OFFSET_FIELD = "offsetField";
  public static final String NAME_CLUSTER_API_KEY = "clusterApiKey";
  public static final String NAME_CLUSTER_API_SECRET = "clusterApiSecret";
  public static final String NAME_SR_URL = "schemaRegistryUrl";
  public static final String NAME_SR_API_KEY = "schemaRegistryApiKey";
  public static final String NAME_SR_API_SECRET = "schemaRegistryApiSecret";
  public static final String NAME_VALUE_FIELD = "valueField";
  public static final String NAME_FORMAT = "format";
  public static final String NAME_KAFKA_PROPERTIES = "kafkaProperties";

  private static final String SEPARATOR = ":";

  private static final long serialVersionUID = 8069169417140954175L;

  @Name(NAME_BROKERS)
  @Description("List of Kafka brokers specified in host1:port1,host2:port2 form. For example, " +
    "host1.example.com:9092,host2.example.com:9092.")
  @Macro
  private final String brokers;

  @Name(NAME_TOPIC)
  @Description("Kafka topic to read from.")
  @Macro
  private final String topic;

  @Name(NAME_PARTITIONS)
  @Description("The topic partitions to read from. If not specified, all partitions will be read.")
  @Nullable
  @Macro
  private final String partitions;

  @Name(NAME_INITIAL_PARTITION_OFFSETS)
  @Description("The initial offset for each topic partition. If this is not specified, " +
    "all partitions will have the same initial offset, which is determined by the defaultInitialOffset property. " +
    "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. " +
    "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read.")
  @Nullable
  @Macro
  private final String initialPartitionOffsets;

  @Name(NAME_DEFAULT_INITIAL_OFFSET)
  @Description("The default initial offset for all topic partitions. " +
    "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. " +
    "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. " +
    "If you wish to set different initial offsets for different partitions, use the Initial Partition Offsets" +
    " property.")
  @Nullable
  @Macro
  private final Long defaultInitialOffset;

  @Name(NAME_SCHEMA)
  @Description("Output schema of the source, including the timeField and keyField. " +
    "The fields excluding the timeField and keyField are used in conjunction with the format " +
    "to parse Kafka payloads.")
  private final String schema;

  @Name(NAME_FORMAT)
  @Description("Optional format of the Kafka event. Any format supported by CDAP is supported. " +
    "For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values. " +
    "If no format is given, Kafka message payloads will be treated as bytes.")
  @Nullable
  private final String format;

  @Name(NAME_TIMEFIELD)
  @Description("Optional name of the field containing the read time of the batch. " +
    "If this is not set, no time field will be added to output records. " +
    "If set, this field must be present in the schema property and must be a long.")
  @Nullable
  private final String timeField;

  @Name(NAME_KEYFIELD)
  @Description("Optional name of the field containing the message key. " +
    "If this is not set, no key field will be added to output records. " +
    "If set, this field must be present in the schema property and must be bytes.")
  @Nullable
  private final String keyField;

  @Name(NAME_PARTITION_FIELD)
  @Description("Optional name of the field containing the kafka partition that was read from. " +
    "If this is not set, no partition field will be added to output records. " +
    "If set, this field must be present in the schema property and must be an integer.")
  @Nullable
  private final String partitionField;

  @Name(NAME_OFFSET_FIELD)
  @Description("Optional name of the field containing the kafka offset that the message was read from. " +
    "If this is not set, no offset field will be added to output records. " +
    "If set, this field must be present in the schema property and must be a long.")
  @Nullable
  private final String offsetField;

  @Name(NAME_MAX_RATE)
  @Description("Max number of records to read per second per partition. 0 means there is no limit. Defaults to 1000.")
  @Nullable
  private final Integer maxRatePerPartition;

  @Name(NAME_KAFKA_PROPERTIES)
  @Description("Additional kafka consumer properties to set.")
  @Macro
  @Nullable
  private final String kafkaProperties;

  @Name(NAME_CLUSTER_API_KEY)
  @Description("The Confluent API Key.")
  @Macro
  private final String clusterApiKey;

  @Name(NAME_CLUSTER_API_SECRET)
  @Description("The Confluent API Secret.")
  @Macro
  private final String clusterApiSecret;

  @Name(NAME_SR_URL)
  @Description("The Schema Registry endpoint URL.")
  @Macro
  @Nullable
  private final String schemaRegistryUrl;

  @Name(NAME_SR_API_KEY)
  @Description("The Schema Registry API Key.")
  @Macro
  @Nullable
  private final String schemaRegistryApiKey;

  @Name(NAME_SR_API_SECRET)
  @Description("The Schema Registry API Secret.")
  @Macro
  @Nullable
  private final String schemaRegistryApiSecret;

  @Name(NAME_VALUE_FIELD)
  @Description("Name of the field containing the message payload. Required when Schema Registry is used." +
    "This field will be used to infer schema from Schema Registry.")
  @Macro
  @Nullable
  private final String valueField;

  public ConfluentStreamingSourceConfig(
    String referenceName,
    String brokers,
    String topic,
    @Nullable String partitions,
    @Nullable String initialPartitionOffsets,
    @Nullable Long defaultInitialOffset,
    String schema,
    @Nullable String format,
    @Nullable String timeField,
    @Nullable String keyField,
    @Nullable String partitionField,
    @Nullable String offsetField,
    @Nullable Integer maxRatePerPartition,
    @Nullable String kafkaProperties,
    String clusterApiKey,
    String clusterApiSecret,
    @Nullable String schemaRegistryUrl,
    @Nullable String schemaRegistryApiKey,
    @Nullable String schemaRegistryApiSecret,
    @Nullable String valueField) {
    super(referenceName);
    this.brokers = brokers;
    this.topic = topic;
    this.partitions = partitions;
    this.initialPartitionOffsets = initialPartitionOffsets;
    this.defaultInitialOffset = defaultInitialOffset;
    this.schema = schema;
    this.format = format;
    this.timeField = timeField;
    this.keyField = keyField;
    this.partitionField = partitionField;
    this.offsetField = offsetField;
    this.maxRatePerPartition = maxRatePerPartition;
    this.kafkaProperties = kafkaProperties;
    this.clusterApiKey = clusterApiKey;
    this.clusterApiSecret = clusterApiSecret;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.schemaRegistryApiKey = schemaRegistryApiKey;
    this.schemaRegistryApiSecret = schemaRegistryApiSecret;
    this.valueField = valueField;
  }

  public String getTopic() {
    return topic;
  }

  public String getBrokers() {
    return brokers;
  }

  @Nullable
  public String getTimeField() {
    return getNullableProperty(timeField);
  }

  @Nullable
  public String getKeyField() {
    return getNullableProperty(keyField);
  }

  @Nullable
  public String getPartitionField() {
    return getNullableProperty(partitionField);
  }

  @Nullable
  public String getOffsetField() {
    return getNullableProperty(offsetField);
  }

  @Nullable
  public String getFormat() {
    return getNullableProperty(format);
  }

  @Nullable
  public Integer getMaxRatePerPartition() {
    return maxRatePerPartition;
  }

  @Nullable
  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema : " + e.getMessage());
    }
  }

  @Nullable
  public Schema getSchema(FailureCollector collector) {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema : " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
    }
    throw collector.getOrThrowException();
  }

  // gets the message schema from the schema field. If the time, key, partition, or offset fields are in the configured
  // schema, they will be removed.
  public Schema getMessageSchema() {
    Schema schema = getSchema();
    List<Schema.Field> messageFields = schema.getFields()
      .stream()
      .filter(field -> {
        String fieldName = field.getName();
        return !fieldName.equals(timeField) && !fieldName.equals(keyField) && !fieldName.equals(partitionField)
          && !fieldName.equals(offsetField);
      })
      .collect(Collectors.toList());
    if (messageFields.isEmpty()) {
      throw new IllegalArgumentException("Schema must contain at least one message field");
    }
    return Schema.recordOf("kafka.message", messageFields);
  }

  // gets the message schema from the schema field. If the time, key, partition, or offset fields are in the configured
  // schema, they will be removed.
  public Schema getMessageSchema(FailureCollector collector) {
    Schema schema = getSchema(collector);
    List<Schema.Field> messageFields = new ArrayList<>();
    boolean timeFieldExists = false;
    boolean keyFieldExists = false;
    boolean partitionFieldExists = false;
    boolean offsetFieldExists = false;

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
      Schema.Type fieldType = fieldSchema.getType();
      // if the field is not the time field and not the key field, it is a message field.
      if (fieldName.equals(timeField)) {
        if (fieldType != Schema.Type.LONG || fieldSchema.getLogicalType() != null) {
          collector.addFailure("The time field must be of type long or nullable long.", null)
            .withConfigProperty(NAME_TIMEFIELD).withOutputSchemaField(timeField);
        }
        timeFieldExists = true;
      } else if (fieldName.equals(keyField)) {
        if (getSchemaRegistryUrl() == null && ((fieldType != Schema.Type.STRING && fieldType != Schema.Type.BYTES)
          || fieldSchema.getLogicalType() != null)) {
          collector.addFailure("The key field must be of type bytes, nullable bytes, string, nullable string.", null)
            .withConfigProperty(NAME_KEYFIELD).withOutputSchemaField(keyField);
        }
        keyFieldExists = true;
      } else if (fieldName.equals(partitionField)) {
        if (fieldType != Schema.Type.INT || fieldSchema.getLogicalType() != null) {
          collector.addFailure("The partition field must be of type int.", null)
            .withConfigProperty(NAME_PARTITION_FIELD).withOutputSchemaField(partitionField);
        }
        partitionFieldExists = true;
      } else if (fieldName.equals(offsetField)) {
        if (fieldType != Schema.Type.LONG || fieldSchema.getLogicalType() != null) {
          collector.addFailure("The offset field must be of type long.", null)
            .withConfigProperty(NAME_OFFSET_FIELD).withOutputSchemaField(offsetField);
        }
        offsetFieldExists = true;
      } else {
        messageFields.add(field);
      }
    }

    if (getTimeField() != null && !timeFieldExists) {
      collector.addFailure(String.format("Time field '%s' must exist in schema.", timeField), null)
        .withConfigProperty(NAME_TIMEFIELD);
    }
    if (getKeyField() != null && !keyFieldExists) {
      collector.addFailure(String.format("Key field '%s' must exist in schema.", keyField), null)
        .withConfigProperty(NAME_KEYFIELD);
    }
    if (getPartitionField() != null && !partitionFieldExists) {
      collector.addFailure(String.format("Partition field '%s' must exist in schema.", partitionField), null)
        .withConfigProperty(NAME_PARTITION_FIELD);
    }
    if (getOffsetField() != null && !offsetFieldExists) {
      collector.addFailure(String.format("Offset field '%s' must exist in schema.", offsetField), null)
        .withConfigProperty(NAME_OFFSET_FIELD);
    }

    if (messageFields.isEmpty()) {
      collector.addFailure("Schema must contain at least one message field.", null);
      throw collector.getOrThrowException();
    }
    return Schema.recordOf("kafka.message", messageFields);
  }

  /**
   * Get the initial partition offsets for the specified partitions. If an initial offset is specified in the
   * initialPartitionOffsets property, that value will be used. Otherwise, the defaultInitialOffset will be used.
   *
   * @param partitionsToRead the partitions to read
   * @param collector        failure collector
   * @return initial partition offsets.
   */
  public Map<TopicPartition, Long> getInitialPartitionOffsets(Set<Integer> partitionsToRead,
                                                              FailureCollector collector) {
    Map<TopicPartition, Long> partitionOffsets = new HashMap<>();

    // set default initial partitions
    for (Integer partition : partitionsToRead) {
      partitionOffsets.put(new TopicPartition(topic, partition), defaultInitialOffset);
    }

    // if initial partition offsets are specified, overwrite the defaults.
    if (initialPartitionOffsets != null) {
      for (KeyValue<String, String> partitionAndOffset : KeyValueListParser.DEFAULT.parse(initialPartitionOffsets)) {
        String partitionStr = partitionAndOffset.getKey();
        String offsetStr = partitionAndOffset.getValue();
        int partition;
        try {
          partition = Integer.parseInt(partitionStr);
        } catch (NumberFormatException e) {
          collector.addFailure(
            String.format("Invalid partition '%s' in initialPartitionOffsets.", partitionStr),
            "Partition must be a valid integer.")
            .withConfigElement(NAME_INITIAL_PARTITION_OFFSETS, partitionStr + SEPARATOR + offsetStr);
          continue;
        }
        long offset;
        try {
          offset = Long.parseLong(offsetStr);
        } catch (NumberFormatException e) {
          collector.addFailure(
            String.format("Invalid offset '%s' in initialPartitionOffsets for partition %d.", offsetStr, partition),
            "Offset muse be a valid integer.")
            .withConfigElement(NAME_INITIAL_PARTITION_OFFSETS, partitionStr + SEPARATOR + offsetStr);
          continue;
        }
        partitionOffsets.put(new TopicPartition(topic, partition), offset);
      }
    }

    return partitionOffsets;
  }

  /**
   * @return set of partitions to read from. Returns an empty list if no partitions were specified.
   */
  public Set<Integer> getPartitions(FailureCollector collector) {
    Set<Integer> partitionSet = new HashSet<>();
    if (Strings.isNullOrEmpty(partitions)) {
      return partitionSet;
    }
    for (String partition : Splitter.on(',').trimResults().split(partitions)) {
      try {
        partitionSet.add(Integer.parseInt(partition));
      } catch (NumberFormatException e) {
        collector.addFailure(String.format("Invalid partition '%s'.", partition), "Partitions must be integers.")
          .withConfigElement(NAME_PARTITIONS, partition);
      }
    }
    return partitionSet;
  }

  public String getClusterApiKey() {
    return clusterApiKey;
  }

  public String getClusterApiSecret() {
    return clusterApiSecret;
  }

  @Nullable
  public String getSchemaRegistryUrl() {
    return getNullableProperty(schemaRegistryUrl);
  }

  @Nullable
  public String getSchemaRegistryApiKey() {
    return getNullableProperty(schemaRegistryApiKey);
  }

  @Nullable
  public String getSchemaRegistryApiSecret() {
    return getNullableProperty(schemaRegistryApiSecret);
  }

  @Nullable
  public String getValueField() {
    return getNullableProperty(valueField);
  }

  @Nullable
  private String getNullableProperty(String property) {
    return Strings.isNullOrEmpty(property) ? null : property;
  }

  public Map<String, String> getKafkaProperties() {
    Map<String, String> conf = new HashMap<>();
    if (!Strings.isNullOrEmpty(kafkaProperties)) {
      KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
      for (KeyValue<String, String> keyVal : kvParser.parse(kafkaProperties)) {
        conf.put(keyVal.getKey(), keyVal.getValue());
      }
    }
    return conf;
  }

  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);
    // brokers can be null since it is macro enabled.
    if (!containsMacro(NAME_BROKERS)) {
      ConfigValidations.validateBrokers(brokers, NAME_BROKERS, collector);
    }
    Set<Integer> partitions = getPartitions(collector);
    getInitialPartitionOffsets(partitions, collector);

    if (maxRatePerPartition == null) {
      collector.addFailure("Max rate per partition must be provided.", null)
        .withConfigProperty(NAME_MAX_RATE);
    } else if (maxRatePerPartition < 0) {
      collector.addFailure(String.format("Invalid maxRatePerPartition '%d'.", maxRatePerPartition),
                           "Rate must be 0 or greater.")
        .withConfigProperty(NAME_MAX_RATE);
    }

    if (!Strings.isNullOrEmpty(timeField) && !Strings.isNullOrEmpty(keyField) && timeField.equals(keyField)) {
      collector.addFailure(String.format(
        "The timeField and keyField cannot both have the same name (%s).", timeField), null)
        .withConfigProperty(NAME_TIMEFIELD).withConfigProperty(NAME_KEYFIELD);
    }

    if (!containsMacro(NAME_CLUSTER_API_KEY) && Strings.isNullOrEmpty(clusterApiKey)) {
      collector.addFailure("Cluster API Key must be provided.", null)
        .withConfigProperty(NAME_CLUSTER_API_KEY);
    }

    if (!containsMacro(NAME_CLUSTER_API_SECRET) && Strings.isNullOrEmpty(clusterApiSecret)) {
      collector.addFailure("Cluster API Secret must be provided.", null)
        .withConfigProperty(NAME_CLUSTER_API_SECRET);
    }

    if (!Strings.isNullOrEmpty(schemaRegistryUrl)) {
      if (!Strings.isNullOrEmpty(format)) {
        collector.addFailure("Message Format may not be used with Schema Registry.", null)
          .withConfigProperty(NAME_SR_URL)
          .withConfigProperty(NAME_FORMAT);
      }
      if (!containsMacro(NAME_SR_API_KEY) && Strings.isNullOrEmpty(clusterApiKey)) {
        collector.addFailure("Schema Registry API Key must be provided.", null)
          .withConfigProperty(NAME_SR_API_KEY);
      }
      if (!containsMacro(NAME_SR_API_SECRET) && Strings.isNullOrEmpty(clusterApiSecret)) {
        collector.addFailure("Schema Registry API Secret must be provided.", null)
          .withConfigProperty(NAME_SR_API_SECRET);
      }
      if (!containsMacro(NAME_VALUE_FIELD) && Strings.isNullOrEmpty(valueField)) {
        collector.addFailure("Message Field should be provided when Schema Registry is used.", null)
          .withConfigProperty(NAME_VALUE_FIELD);
      }
    } else if (!Strings.isNullOrEmpty(format)) {
      // it is a format, make sure we can instantiate it.
      Schema messageSchema = getMessageSchema(collector);
      FormatSpecification formatSpec = new FormatSpecification(format, messageSchema, new HashMap<>());
      try {
        RecordFormats.createInitializedFormat(formatSpec);
      } catch (Exception e) {
        collector.addFailure(String.format(
          "Unable to instantiate a message parser from format '%s': %s",
          format, e.getMessage()), null).withStacktrace(e.getStackTrace()).withConfigProperty(NAME_FORMAT);
      }
    } else if (!containsMacro(NAME_SR_URL) && !containsMacro(NAME_FORMAT)) {
      // if format is empty, there must be just a single message field of type bytes or nullable types.
      Schema messageSchema = getMessageSchema(collector);
      List<Schema.Field> messageFields = messageSchema.getFields();
      if (messageFields.size() > 1) {
        for (Schema.Field messageField : messageFields) {
          collector.addFailure(
            "Without a format, the schema must contain just a single message field of type bytes or nullable bytes.",
            String.format("Remove field '%s'.", messageField.getName()))
            .withOutputSchemaField(messageField.getName()).withConfigProperty(NAME_FORMAT);
        }
        return;
      }

      Schema.Field messageField = messageFields.get(0);
      Schema messageFieldSchema = messageField.getSchema().isNullable() ? messageField.getSchema().getNonNullable() :
        messageField.getSchema();
      Schema.Type messageFieldType = messageFieldSchema.getType();
      if (messageFieldType != Schema.Type.BYTES || messageFieldSchema.getLogicalType() != null) {
        collector.addFailure(
          String.format("Without a format, the message field must be of type bytes or nullable bytes, " +
                          "but field '%s' is of type '%s'.",
                        messageField.getName(), messageField.getSchema().getDisplayName()), null)
          .withOutputSchemaField(messageField.getName()).withConfigProperty(NAME_FORMAT);
      }
    }
  }
}
