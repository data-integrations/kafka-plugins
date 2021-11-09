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

package io.cdap.plugin.confluent.integration;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.avro.StructuredToAvroTransformer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaTestUtils {
  private static final String KAFKA_SERVER = requireProperty("test.kafka_server");
  private static final String CLUSTER_API_KEY = requireProperty("test.cluster_api_key");
  private static final String CLUSTER_API_SECRET = requireProperty("test.cluster_api_secret");
  public static final String SR_URL = requireProperty("test.schema_registry_url");
  public static final String SR_API_KEY = requireProperty("test.schema_registry_api_key");
  public static final String SR_API_SECRET = requireProperty("test.schema_registry_api_secret");

  private KafkaTestUtils() {
    throw new AssertionError("Can not be initialized");
  }

  public static Consumer<String, String> createConsumer() {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    props.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, "500");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.putAll(getSecurityProps());
    return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
  }

  public static Consumer<Object, Object> createConsumerForSchemaRegistry() {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    props.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, "500");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.putAll(getSecurityProps());
    props.put("schema.registry.url", SR_URL);
    props.put("basic.auth.credentials.source", "USER_INFO");
    props.put("schema.registry.basic.auth.user.info", SR_API_KEY + ":" + SR_API_SECRET);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getCanonicalName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getCanonicalName());
    return new KafkaConsumer<>(props);
  }

  public static KafkaProducer<byte[], byte[]> createProducer() {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    props.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, "500");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.putAll(getSecurityProps());
    return new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
  }

  public static KafkaProducer<Object, Object> createProducerForSchemaRegistry() {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    props.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, "500");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.putAll(getSecurityProps());
    props.put("schema.registry.url", SR_URL);
    props.put("basic.auth.credentials.source", "USER_INFO");
    props.put("schema.registry.basic.auth.user.info", SR_API_KEY + ":" + SR_API_SECRET);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getCanonicalName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getCanonicalName());
    return new KafkaProducer<>(props);
  }

  // Create topic in Confluent Cloud
  public static void createTopic(String topic, int partitions, int replication) {
    NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
    try (AdminClient adminClient = createAdminClient()) {
      adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
    } catch (InterruptedException | ExecutionException e) {
      // Ignore if TopicExistsException, which may be valid if topic exists
      if (!(e.getCause() instanceof TopicExistsException)) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void deleteTopic(String topic) {
    try (AdminClient adminClient = createAdminClient()) {
      adminClient.deleteTopics(Collections.singletonList(topic)).all().get();
    } catch (InterruptedException | ExecutionException e) {
      // Ignore if UnknownTopicOrPartitionException, which may be valid if topic does not exist
      if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
        throw new RuntimeException(e);
      }
    }
  }

  public static GenericRecord toGenericRecord(StructuredRecord structuredRecord, Schema schema) {
    StructuredToAvroTransformer transformer = new StructuredToAvroTransformer(schema);
    try {
      return transformer.transform(structuredRecord);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to convert records", e);
    }
  }

  public static List<GenericRecord> toGenericRecords(List<StructuredRecord> structuredRecords, Schema schema) {
    StructuredToAvroTransformer transformer = new StructuredToAvroTransformer(schema);
    try {
      List<GenericRecord> genericRecords = new ArrayList<>();
      for (StructuredRecord structuredRecord : structuredRecords) {
        genericRecords.add(transformer.transform(structuredRecord));
      }
      return genericRecords;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to convert records", e);
    }
  }

  private static String requireProperty(String propertyName) {
    return Objects.requireNonNull(System.getProperty(propertyName),
                                  String.format("System property '%s' should be provided", propertyName));
  }

  private static AdminClient createAdminClient() {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    props.putAll(getSecurityProps());
    return AdminClient.create(props);
  }

  private static Map<String, Object> getSecurityProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required " +
      "username=" + CLUSTER_API_KEY + " password=" + CLUSTER_API_SECRET + ";");
    return props;
  }
}
