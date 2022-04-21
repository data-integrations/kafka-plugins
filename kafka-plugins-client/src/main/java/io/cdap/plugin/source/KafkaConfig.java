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

package io.cdap.plugin.source;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.common.KafkaHelpers;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.ReferencePluginConfig;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Conf for Kafka streaming source.
 */
@SuppressWarnings("unused")
public class KafkaConfig extends ReferencePluginConfig implements Serializable {
  private static final String NAME_SCHEMA = "schema";
  private static final String NAME_BROKERS = "brokers";
  private static final String NAME_PARTITIONS = "partitions";
  private static final String NAME_MAX_RATE = "maxRatePerPartition";
  private static final String NAME_INITIAL_PARTITION_OFFSETS = "initialPartitionOffsets";
  private static final String NAME_TIMEFIELD = "timeField";
  private static final String NAME_KEYFIELD = "keyField";
  private static final String NAME_PARTITION_FIELD = "partitionField";
  private static final String NAME_OFFSET_FIELD = "offsetField";
  private static final String NAME_FORMAT = "format";
  private static final String SEPARATOR = ":";
  public static final String OFFSET_START_FROM_BEGINNING = "Start from beginning";
  public static final String OFFSET_START_FROM_LAST_OFFSET = "Start from last processed offset";
  public static final String OFFSET_START_FROM_SPECIFIC_OFFSET = "Start from specific offset";

  private static final long serialVersionUID = 8069169417140954175L;

  @Description("List of Kafka brokers specified in host1:port1,host2:port2 form. For example, " +
    "host1.example.com:9092,host2.example.com:9092.")
  @Macro
  private String brokers;

  @Description("Kafka topic to read from.")
  @Macro
  private String topic;

  @Description("The topic partitions to read from. If not specified, all partitions will be read.")
  @Nullable
  @Macro
  private String partitions;

  @Description("The initial offset for each topic partition. If this is not specified, " +
    "all partitions will have the same initial offset, which is determined by the defaultInitialOffset property. " +
    "An offset of -2 means the smallest offset. An offset of -1 means the latest offset. " +
    "Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read.")
  @Nullable
  @Macro
  private String initialPartitionOffsets;

  @Description("The default initial offset for all topic partitions. Defaults to latest offset.")
  @Nullable
  @Macro
  private Long defaultInitialOffset;

  @Description("The initial offset for all topic partitions. " +
    "Start from beginning means the smallest offset will be set. " +
    "Start from last processed offset means the latest offset will be set. Defaults to null. " +
    "If start from specific offset is selected default initial offset must be provided." +
    "If you wish to set different initial offsets for different partitions, use the initialPartitionOffsets property.")
  @Nullable
  @Macro
  private String initialOffset;

  @Description("Output schema of the source, including the timeField and keyField. " +
    "The fields excluding the timeField and keyField are used in conjunction with the format " +
    "to parse Kafka payloads.")
  private String schema;

  @Description("Optional format of the Kafka event. Any format supported by CDAP is supported. " +
    "For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values. " +
    "If no format is given, Kafka message payloads will be treated as bytes.")
  @Nullable
  private String format;

  @Description("Optional name of the field containing the read time of the batch. " +
    "If this is not set, no time field will be added to output records. " +
    "If set, this field must be present in the schema property and must be a long.")
  @Nullable
  private String timeField;

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

  @Description("Max number of records to read per second per partition. 0 means there is no limit. Defaults to 1000.")
  @Nullable
  private Integer maxRatePerPartition;

  @Description("Additional kafka consumer properties to set.")
  @Macro
  @Nullable
  private String kafkaProperties;

  @Description("The kerberos principal used for the source when kerberos security is enabled for kafka.")
  @Macro
  @Nullable
  private String principal;

  @Description("The keytab location for the kerberos principal when kerberos security is enabled for kafka.")
  @Macro
  @Nullable
  private String keytabLocation;

  public KafkaConfig() {
    super("");
    defaultInitialOffset = -1L;
    maxRatePerPartition = 1000;
  }

  public String getTopic() {
    return topic;
  }

  public String getBrokers() {
    return brokers;
  }

  @Nullable
  public String getTimeField() {
    return Strings.isNullOrEmpty(timeField) ? null : timeField;
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

  @Nullable
  public Integer getMaxRatePerPartition() {
    return maxRatePerPartition;
  }

  @Nullable
  public Long getDefaultInitialOffset() {
    if (!containsMacro(initialOffset) && !Strings.isNullOrEmpty(initialOffset)) {
      if (!initialOffset.equals(OFFSET_START_FROM_SPECIFIC_OFFSET)) {
        if (initialOffset.equals(OFFSET_START_FROM_BEGINNING)) {
          return -2L;
        }
        if (initialOffset.equals(OFFSET_START_FROM_LAST_OFFSET)) {
          return -1L;
        }
      }
    }
    return defaultInitialOffset;
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
          throw new IllegalArgumentException("The time field must be of type long or nullable long.");
        }
        timeFieldExists = true;
      } else if (fieldName.equals(keyField)) {
        if (fieldType != Schema.Type.BYTES || fieldSchema.getLogicalType() != null) {
          throw new IllegalArgumentException("The key field must be of type bytes or nullable bytes.");
        }
        keyFieldExists = true;
      } else if (fieldName.equals(partitionField)) {
        if (fieldType != Schema.Type.INT || fieldSchema.getLogicalType() != null) {
          throw new IllegalArgumentException("The partition field must be of type int.");
        }
        partitionFieldExists = true;
      } else if (fieldName.equals(offsetField)) {
        if (fieldType != Schema.Type.LONG || fieldSchema.getLogicalType() != null) {
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

    if (getTimeField() != null && !timeFieldExists) {
      throw new IllegalArgumentException(String.format(
        "timeField '%s' does not exist in the schema. Please add it to the schema.", timeField));
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
        "offsetField '%s' does not exist in the schema. Please add it to the schema.", offsetFieldExists));
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
        if (fieldType != Schema.Type.BYTES || fieldSchema.getLogicalType() != null) {
          collector.addFailure("The key field must be of type bytes or nullable bytes.", null)
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
      collector.addFailure("Schema must contain at least one other field besides the time and key fields.", null);
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
    final Long defaultInitialOffset = getDefaultInitialOffset();
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
   * @return broker host to broker port mapping.
   */
  public Map<String, Integer> getBrokerMap(FailureCollector collector) {
    Map<String, Integer> brokerMap = new HashMap<>();
    try {
      Iterable<KeyValue<String, String>> parsed = KeyValueListParser.DEFAULT.parse(brokers);
      for (KeyValue<String, String> hostAndPort : parsed) {
        String host = hostAndPort.getKey();
        String portStr = hostAndPort.getValue();
        try {
          brokerMap.put(host, Integer.parseInt(portStr));
        } catch (NumberFormatException e) {
          collector.addFailure(String.format("Invalid port '%s' for host '%s'.", portStr, host),
                               "It should be a valid port number.")
            .withConfigElement(NAME_BROKERS, host + SEPARATOR + portStr);
        }
      }
    } catch (IllegalArgumentException e) {
      // no-op
    }

    if (brokerMap.isEmpty()) {
      collector.addFailure("Kafka brokers must be provided in host:port format.", null)
        .withConfigProperty(NAME_BROKERS);
    }
    return brokerMap;
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

  @Nullable
  public String getPrincipal() {
    return principal;
  }

  @Nullable
  public String getKeytabLocation() {
    return keytabLocation;
  }

  public Map<String, String> getKafkaProperties() {
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    Map<String, String> conf = new HashMap<>();
    if (!Strings.isNullOrEmpty(kafkaProperties)) {
      for (KeyValue<String, String> keyVal : kvParser.parse(kafkaProperties)) {
        conf.put(keyVal.getKey(), keyVal.getValue());
      }
    }
    return conf;
  }

  public void validate(FailureCollector collector) {
    // brokers can be null since it is macro enabled.
    if (!Strings.isNullOrEmpty(brokers)) {
      getBrokerMap(collector);
    }
    Set<Integer> partitions = getPartitions(collector);
    getInitialPartitionOffsets(partitions, collector);

    if (maxRatePerPartition == null) {
      collector.addFailure("Max rate per partition must be provided.", null)
        .withConfigProperty(NAME_MAX_RATE);
    }

    if (maxRatePerPartition < 0) {
      collector.addFailure(String.format("Invalid maxRatePerPartition '%d'.", maxRatePerPartition),
                           "Rate must be 0 or greater.").withConfigProperty(NAME_MAX_RATE);
    }

    if (!Strings.isNullOrEmpty(timeField) && !Strings.isNullOrEmpty(keyField) && timeField.equals(keyField)) {
      collector.addFailure(String.format(
        "The timeField and keyField cannot both have the same name (%s).", timeField), null)
        .withConfigProperty(NAME_TIMEFIELD).withConfigProperty(NAME_KEYFIELD);
    }

    Schema messageSchema = getMessageSchema(collector);
    // if format is empty, there must be just a single message field of type bytes or nullable types.
    if (Strings.isNullOrEmpty(format)) {
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
                          "but field %s is of type %s.",
                        messageField.getName(), messageField.getSchema().getDisplayName()), null)
          .withOutputSchemaField(messageField.getName()).withConfigProperty(NAME_FORMAT);
      }
    } else {
      // otherwise, if there is a format, make sure we can instantiate it.
      FormatSpecification formatSpec = new FormatSpecification(format, messageSchema, new HashMap<>());
      try {
        RecordFormats.createInitializedFormat(formatSpec);
      } catch (Exception e) {
        collector.addFailure(String.format(
          "Unable to instantiate a message parser from format '%s': %s",
          format, e.getMessage()), null).withStacktrace(e.getStackTrace()).withConfigProperty(NAME_FORMAT);
      }
    }

    KafkaHelpers.validateKerberosSetting(principal, keytabLocation, collector);
  }
}
