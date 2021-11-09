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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.confluent.common.ConfigValidations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Conf for Confluent Kafka streaming sink.
 */
@SuppressWarnings("unused")
public class ConfluentStreamingSinkConfig extends ReferencePluginConfig implements Serializable {
  public static final String NAME_BROKERS = "brokers";
  public static final String NAME_ASYNC = "async";
  public static final String NAME_TIME_FIELD = "timeField";
  public static final String NAME_KEY_FIELD = "keyField";
  public static final String NAME_PARTITION_FIELD = "partitionField";
  public static final String NAME_TOPIC = "topic";
  public static final String NAME_CLUSTER_API_KEY = "clusterApiKey";
  public static final String NAME_CLUSTER_API_SECRET = "clusterApiSecret";
  public static final String NAME_SR_URL = "schemaRegistryUrl";
  public static final String NAME_SR_API_KEY = "schemaRegistryApiKey";
  public static final String NAME_SR_API_SECRET = "schemaRegistryApiSecret";
  public static final String NAME_FORMAT = "format";
  public static final String NAME_COMPRESSION_TYPE = "compressionType";
  public static final String NAME_KAFKA_PROPERTIES = "kafkaProperties";

  public static final Set<String> SUPPORTED_FORMATS = ImmutableSet.of("csv", "json");

  @Name(NAME_BROKERS)
  @Description("Specifies the connection string where Producer can find one or more brokers to " +
    "determine the leader for each topic")
  @Macro
  private final String brokers;

  @Name(NAME_ASYNC)
  @Description("Specifies whether an acknowledgment is required from broker that message was received. " +
    "Default is FALSE")
  @Macro
  private final Boolean async;

  @Name(NAME_TIME_FIELD)
  @Description("Optional name of the field containing the read time of the message. " +
    "If this is not set, message will be send with current timestamp. " +
    "If set, this field must be present in the input schema and must be a long.")
  @Nullable
  private final String timeField;

  @Name(NAME_KEY_FIELD)
  @Description("Specify the key field to be used in the message. Only String Partitioner is supported.")
  @Macro
  @Nullable
  private final String keyField;

  @Name(NAME_PARTITION_FIELD)
  @Description("Optional name of the field containing the partition the message should be written to.\n" +
    "If this is not set, default partition will be used for all messages.\n" +
    "If set, this field must be present in the schema property and must be an int.")
  @Nullable
  private final String partitionField;

  @Name(NAME_TOPIC)
  @Description("Topic to which message needs to be published")
  @Macro
  private final String topic;

  @Name(NAME_FORMAT)
  @Description("Format a structured record should be converted to")
  @Macro
  @Nullable
  private final String format;

  @Name(NAME_KAFKA_PROPERTIES)
  @Description("Additional kafka producer properties to set")
  @Macro
  @Nullable
  private final String kafkaProperties;

  @Name(NAME_COMPRESSION_TYPE)
  @Description("Compression type to be applied on message")
  @Macro
  private final String compressionType;

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

  public ConfluentStreamingSinkConfig(
    String referenceName,
    String brokers,
    Boolean async,
    @Nullable String timeField,
    @Nullable String keyField,
    @Nullable String partitionField,
    String topic,
    @Nullable String format,
    @Nullable String kafkaProperties,
    String compressionType,
    String clusterApiKey,
    String clusterApiSecret,
    @Nullable String schemaRegistryUrl,
    @Nullable String schemaRegistryApiKey,
    @Nullable String schemaRegistryApiSecret
  ) {
    super(referenceName);
    this.brokers = brokers;
    this.async = async;
    this.timeField = timeField;
    this.keyField = keyField;
    this.partitionField = partitionField;
    this.topic = topic;
    this.format = format;
    this.kafkaProperties = kafkaProperties;
    this.compressionType = compressionType;
    this.clusterApiKey = clusterApiKey;
    this.clusterApiSecret = clusterApiSecret;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.schemaRegistryApiKey = schemaRegistryApiKey;
    this.schemaRegistryApiSecret = schemaRegistryApiSecret;
  }

  public void validate(Schema inputSchema, FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);

    // brokers can be null since it is macro enabled.
    if (!containsMacro(NAME_BROKERS)) {
      ConfigValidations.validateBrokers(brokers, NAME_BROKERS, collector);
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
      Schema messageSchema = getMessageSchema(inputSchema, collector);
      List<Schema.Field> messageFields = messageSchema.getFields();
      if (messageFields.size() > 1) {
        for (Schema.Field messageField : messageFields) {
          collector.addFailure(
            "Using Schema Registry, the schema must contain just a single message field.",
            String.format("Remove field '%s'.", messageField.getName()))
            .withInputSchemaField(messageField.getName()).withConfigProperty(NAME_FORMAT);
        }
      }
    } else if (!Strings.isNullOrEmpty(format)) {
      Schema messageSchema = getMessageSchema(inputSchema, collector);
      if (!SUPPORTED_FORMATS.contains(format.toLowerCase())) {
        String supportedFormatsString = String.join(",", SUPPORTED_FORMATS);
        collector.addFailure(String.format(
          "Unsupported message format '%s'. Supported formats are: '%s'.", format, supportedFormatsString), null)
          .withConfigProperty(NAME_FORMAT);
      }
    } else if (!containsMacro(NAME_SR_URL) && !containsMacro(NAME_FORMAT)) {
      // if format is empty, there must be just a single message field of type bytes or nullable types.
      Schema messageSchema = getMessageSchema(inputSchema, collector);
      List<Schema.Field> messageFields = messageSchema.getFields();
      if (messageFields.size() > 1) {
        for (Schema.Field messageField : messageFields) {
          collector.addFailure(
            "Without a format, the schema must contain just a single message field of type bytes or nullable bytes.",
            String.format("Remove field '%s'.", messageField.getName()))
            .withInputSchemaField(messageField.getName()).withConfigProperty(NAME_FORMAT);
        }
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
          .withInputSchemaField(messageField.getName()).withConfigProperty(NAME_FORMAT);
      }
    }
  }

  public String getBrokers() {
    return brokers;
  }

  public Boolean getAsync() {
    return async;
  }

  public String getTopic() {
    return topic;
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


  public String getCompressionType() {
    return compressionType;
  }

  public String getClusterApiKey() {
    return clusterApiKey;
  }

  public String getClusterApiSecret() {
    return clusterApiSecret;
  }

  @Nullable
  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  @Nullable
  public String getSchemaRegistryApiKey() {
    return schemaRegistryApiKey;
  }

  @Nullable
  public String getSchemaRegistryApiSecret() {
    return schemaRegistryApiSecret;
  }

  public String getFormat() {
    return format;
  }

  @Nullable
  public String getTimeField() {
    return timeField;
  }

  @Nullable
  public String getKeyField() {
    return keyField;
  }

  @Nullable
  public String getPartitionField() {
    return partitionField;
  }

  // gets the message schema from the schema field. If the key or partition fields are in the configured
  // schema, they will be removed.
  public Schema getMessageSchema(Schema schema) {
    List<Schema.Field> messageFields = schema.getFields()
      .stream()
      .filter(field -> !field.getName().equals(keyField) && !field.getName().equals(partitionField)
        && !field.getName().equals(timeField))
      .collect(Collectors.toList());
    if (messageFields.isEmpty()) {
      throw new IllegalArgumentException("Schema must contain at least one message field");
    }
    return Schema.recordOf("kafka.message", messageFields);
  }

  // gets the message schema from the schema field. If the key, or partition fields are in the configured
  // schema, they will be removed.
  public Schema getMessageSchema(Schema schema, FailureCollector collector) {
    List<Schema.Field> messageFields = new ArrayList<>();
    boolean timeFieldExists = false;
    boolean keyFieldExists = false;
    boolean partitionFieldExists = false;

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
      Schema.Type fieldType = fieldSchema.getType();
      if (fieldName.equals(timeField)) {
        if (fieldType != Schema.Type.LONG || fieldSchema.getLogicalType() != null) {
          collector.addFailure("The time field must be of type long or nullable long.", null)
            .withConfigProperty(NAME_TIME_FIELD).withOutputSchemaField(timeField);
        }
        timeFieldExists = true;
      } else if (fieldName.equals(keyField)) {
        if ((fieldType != Schema.Type.STRING && fieldType != Schema.Type.BYTES)
          || fieldSchema.getLogicalType() != null) {
          collector.addFailure("The key field must be of type bytes, nullable bytes, string, nullable string.", null)
            .withConfigProperty(NAME_KEY_FIELD)
            .withInputSchemaField(keyField);
        }
        keyFieldExists = true;
      } else if (fieldName.equals(partitionField)) {
        if (fieldType != Schema.Type.INT || fieldSchema.getLogicalType() != null) {
          collector.addFailure("The partition field must be of type int.", null)
            .withConfigProperty(NAME_PARTITION_FIELD)
            .withInputSchemaField(partitionField);
        }
        partitionFieldExists = true;
      } else {
        messageFields.add(field);
      }
    }

    if (!Strings.isNullOrEmpty(timeField) && !timeFieldExists) {
      collector.addFailure(String.format("Time field '%s' must exist in schema.", timeField), null)
        .withConfigProperty(NAME_TIME_FIELD);
    }
    if (!Strings.isNullOrEmpty(keyField) && !keyFieldExists) {
      collector.addFailure(String.format("Key field '%s' must exist in schema.", keyField), null)
        .withConfigProperty(NAME_KEY_FIELD);
    }
    if (!Strings.isNullOrEmpty(partitionField) && !partitionFieldExists) {
      collector.addFailure(String.format("Partition field '%s' must exist in schema.", partitionField), null)
        .withConfigProperty(NAME_PARTITION_FIELD);
    }

    if (messageFields.isEmpty()) {
      collector.addFailure("Schema must contain at least one message field.", null);
      throw collector.getOrThrowException();
    }
    return Schema.recordOf("kafka.message", messageFields);
  }
}
