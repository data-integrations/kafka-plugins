/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.plugin.connector;

import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.batch.source.KafkaBatchConfig;
import io.cdap.plugin.batch.source.KafkaBatchSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Kafka Connector
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(KafkaConnector.NAME)
@Category("Messaging Systems")
@Description("Kafka Connector to browse and sample topics using kafka 0.10.2 api")
public class KafkaConnector implements DirectConnector {
  public static final String NAME = "Kafka";
  static final String TOPIC_TYPE = "topic";
  private static final long TIME_OUT_MS = 15000L;
  private static final String MESSAGE_FIELD = "message";
  private static final Schema DEFAULT_SCHEMA =
    Schema.recordOf("kafka", Schema.Field.of(MESSAGE_FIELD, Schema.of(Schema.Type.STRING)));
  private final KafkaConnectorConfig config;

  public KafkaConnector(KafkaConnectorConfig config) {
    this.config = config;
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext connectorContext,
                                       SampleRequest sampleRequest) {
    String topic = cleanse(sampleRequest.getPath());
    if (topic.isEmpty()) {
      throw new IllegalArgumentException("Topic is not provided in the path");
    }

    int limit = sampleRequest.getLimit();
    try (KafkaConsumer<String, String> consumer = getKafkaConsumer()) {
      consumer.subscribe(Collections.singleton(topic));
      List<StructuredRecord> structuredRecords = new ArrayList<>();
      ConsumerRecords<String, String> records = consumer.poll(TIME_OUT_MS);
      for (ConsumerRecord<String, String> record : records) {
        if (structuredRecords.size() >= limit) {
          break;
        }

        structuredRecords.add(StructuredRecord.builder(DEFAULT_SCHEMA).set(MESSAGE_FIELD, record.value()).build());
      }
      return structuredRecords;
    }
  }

  @Override
  public void test(ConnectorContext connectorContext) throws ValidationException {
    try (KafkaConsumer<String, String> consumer = getKafkaConsumer()) {
      consumer.listTopics();
    }
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest request) {
    String path = cleanse(request.getPath());
    int limit = request.getLimit() == null || request.getLimit() <= 0 ? Integer.MAX_VALUE : request.getLimit();
    BrowseDetail.Builder builder = BrowseDetail.builder();
    try (KafkaConsumer<String, String> consumer = getKafkaConsumer()) {
      Set<String> topics = consumer.listTopics().keySet();
      // not root, then it is topic layer, check if it exists and return
      if (!path.isEmpty()) {
        if (!topics.contains(path)) {
          return builder.build();
        }
        return builder.setTotalCount(1).addEntity(BrowseEntity.builder(path, path, TOPIC_TYPE).build()).build();
      }

      // set limit
      topics.stream().limit(limit)
        .forEach(topic -> builder.addEntity(BrowseEntity.builder(topic, topic, TOPIC_TYPE).build()));
      builder.setTotalCount(topics.size());
      return builder.build();
    }
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest request) {
    Map<String, String> properties = new HashMap<>();
    properties.put(KafkaBatchSource.Kafka10BatchConfig.NAME_USE_CONNECTION, "true");
    properties.put(KafkaBatchSource.Kafka10BatchConfig.NAME_CONNECTION, request.getConnectionWithMacro());
    properties.put(KafkaBatchConfig.FORMAT, "text");
    String topic = cleanse(request.getPath());
    if (!topic.isEmpty()) {
      properties.put(KafkaBatchConfig.TOPIC, topic);
    }
    return ConnectorSpec.builder().setSchema(DEFAULT_SCHEMA)
      .addRelatedPlugin(new PluginSpec(KafkaBatchSource.NAME, BatchSource.PLUGIN_TYPE, properties)).build();
  }

  private KafkaConsumer<String, String> getKafkaConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBrokers());
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "true");
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(TIME_OUT_MS));

    // kafka will first use thread classloader to load the serializer, this might not contain the serializer,
    // have to set the plugin class loader for it
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      return new KafkaConsumer<>(props);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  private String cleanse(String path) {
    String result = path;
    // remove leading and trailing "/"
    result = result.replaceAll("^/+", "").replaceAll("/+$", "");
    if (result.contains("/")) {
      throw new IllegalArgumentException(String.format("Path %s is invalid, it should only be at root level or " +
                                                         "contain just the topic", path));
    }
    return result;
  }
}
