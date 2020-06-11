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

package io.cdap.plugin.confluent.integration.streaming.sink;

import com.google.common.base.Stopwatch;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.mock.spark.streaming.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.test.SparkManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.confluent.integration.KafkaTestUtils;
import io.cdap.plugin.confluent.integration.streaming.ConfluentStreamingTestBase;
import io.cdap.plugin.confluent.integration.streaming.source.ConfluentStreamingSourceTest;
import io.cdap.plugin.confluent.streaming.sink.ConfluentStreamingSink;
import io.cdap.plugin.confluent.streaming.sink.ConfluentStreamingSinkConfig;
import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSourceConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Confluent Streaming Sink plugin.
 */
public class ConfluentStreamingSinkTest extends ConfluentStreamingTestBase {

  private static Consumer<String, String> kafkaConsumer;
  private static Consumer<Object, Object> kafkaAvroConsumer;

  @Rule
  public TestName testName = new TestName();

  private String topic;
  private SparkManager programManager;

  @BeforeClass
  public static void setupTestClass() {
    kafkaConsumer = KafkaTestUtils.createConsumer();
    kafkaAvroConsumer = KafkaTestUtils.createConsumerForSchemaRegistry();
  }

  @AfterClass
  public static void cleanupTestClass() {
    kafkaConsumer.close();
    kafkaAvroConsumer.close();
  }

  @Before
  public void setUp() {
    topic = ConfluentStreamingSourceTest.class.getSimpleName() + "_" + testName.getMethodName();
    KafkaTestUtils.deleteTopic(topic);
    KafkaTestUtils.createTopic(topic, 2, 3);
    kafkaConsumer.subscribe(Collections.singletonList(topic));
    kafkaAvroConsumer.subscribe(Collections.singletonList(topic));
  }

  @After
  public void tearDown() throws Exception {
    KafkaTestUtils.deleteTopic(topic);
    if (programManager != null) {
      programManager.stop();
      programManager.waitForStopped(10, TimeUnit.SECONDS);
      programManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testWritesWithFormat() throws Exception {
    String keyField = "key";
    String partitionField = "partition";

    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(keyField, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(partitionField, Schema.of(Schema.Type.INT))
    );
    List<StructuredRecord> records = Arrays.asList(
      StructuredRecord.builder(schema)
        .set("id", 1L)
        .set("first", "samuel")
        .set("last", "jackson")
        .set(keyField, "a")
        .set(partitionField, 0)
        .build(),
      StructuredRecord.builder(schema)
        .set("id", 2L)
        .set("first", "dwayne")
        .set("last", "johnson")
        .set(keyField, "b")
        .set(partitionField, 1)
        .build(),
      StructuredRecord.builder(schema)
        .set("id", 3L)
        .set("first", "christopher")
        .set("last", "walken")
        .set(keyField, "c")
        .set(partitionField, 1)
        .build()
    );
    ETLPlugin sourcePlugin = MockSource.getPlugin(schema, records);
    Map<String, String> properties = getConfigProperties();
    properties.put(ConfluentStreamingSourceConfig.NAME_FORMAT, "csv");
    properties.put(ConfluentStreamingSourceConfig.NAME_KEYFIELD, keyField);
    properties.put(ConfluentStreamingSourceConfig.NAME_PARTITION_FIELD, partitionField);
    programManager = deploySourcePlugin(sourcePlugin, properties);
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    List<ConsumerRecord<String, String>> actualRecords = waitForRecordsInKafka(kafkaConsumer, 3);
    SoftAssertions.assertSoftly(softly -> {
      ConsumerRecord<String, String> record1 = findRecordWithKey(actualRecords, "a");
      softly.assertThat(record1.value()).isEqualTo("1,samuel,jackson");
      softly.assertThat(record1.partition()).isEqualTo(0);
      softly.assertThat(record1.offset()).isEqualTo(0);

      ConsumerRecord<String, String> record2 = findRecordWithKey(actualRecords, "b");
      softly.assertThat(record2.value()).isEqualTo("2,dwayne,johnson");
      softly.assertThat(record2.partition()).isEqualTo(1);
      softly.assertThat(record2.offset()).isEqualTo(0);

      ConsumerRecord<String, String> record3 = findRecordWithKey(actualRecords, "c");
      softly.assertThat(record3.value()).isEqualTo("3,christopher,walken");
      softly.assertThat(record3.partition()).isEqualTo(1);
      softly.assertThat(record3.offset()).isEqualTo(1);
    });
  }

  @Test
  public void testWritesWithSchemaRegistry() throws Exception {
    String messageField = "message";
    String keyField = "key";
    String partitionField = "partition";

    Schema valueSchema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING))
    );
    Schema schema = Schema.recordOf(
      "confluent",
      Schema.Field.of(messageField, valueSchema),
      Schema.Field.of(keyField, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(partitionField, Schema.of(Schema.Type.INT))
    );
    StructuredRecord value1 = StructuredRecord.builder(valueSchema)
      .set("id", 1L)
      .set("first", "samuel")
      .set("last", "jackson")
      .build();
    StructuredRecord value2 = StructuredRecord.builder(valueSchema)
      .set("id", 2L)
      .set("first", "dwayne")
      .set("last", "johnson")
      .build();
    StructuredRecord value3 = StructuredRecord.builder(valueSchema)
      .set("id", 3L)
      .set("first", "christopher")
      .set("last", "walken")
      .build();
    List<StructuredRecord> records = Arrays.asList(
      StructuredRecord.builder(schema)
        .set(messageField, value1)
        .set(keyField, "a")
        .set(partitionField, 0)
        .build(),
      StructuredRecord.builder(schema)
        .set(messageField, value2)
        .set(keyField, "b")
        .set(partitionField, 1)
        .build(),
      StructuredRecord.builder(schema)
        .set(messageField, value3)
        .set(keyField, "c")
        .set(partitionField, 1)
        .build()
    );
    ETLPlugin sourcePlugin = MockSource.getPlugin(schema, records);
    Map<String, String> properties = getConfigProperties();
    properties.put(ConfluentStreamingSourceConfig.NAME_KEYFIELD, keyField);
    properties.put(ConfluentStreamingSourceConfig.NAME_PARTITION_FIELD, partitionField);
    properties.put(ConfluentStreamingSourceConfig.NAME_SR_URL, KafkaTestUtils.SR_URL);
    properties.put(ConfluentStreamingSourceConfig.NAME_SR_API_KEY, KafkaTestUtils.SR_API_KEY);
    properties.put(ConfluentStreamingSourceConfig.NAME_SR_API_SECRET, KafkaTestUtils.SR_API_SECRET);
    programManager = deploySourcePlugin(sourcePlugin, properties);
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    GenericRecord expectedValue1 = KafkaTestUtils.toGenericRecord(value1, value1.getSchema());
    GenericRecord expectedValue2 = KafkaTestUtils.toGenericRecord(value2, value2.getSchema());
    GenericRecord expectedValue3 = KafkaTestUtils.toGenericRecord(value3, value3.getSchema());

    List<ConsumerRecord<Object, Object>> actualRecords = waitForRecordsInKafka(kafkaAvroConsumer, 3);
    SoftAssertions.assertSoftly(softly -> {
      ConsumerRecord<Object, Object> record1 = findRecordWithKey(actualRecords, "a");
      softly.assertThat(record1.value()).isEqualTo(expectedValue1);
      softly.assertThat(record1.partition()).isEqualTo(0);
      softly.assertThat(record1.offset()).isEqualTo(0);

      ConsumerRecord<Object, Object> record2 = findRecordWithKey(actualRecords, "b");
      softly.assertThat(record2.value()).isEqualTo(expectedValue2);
      softly.assertThat(record2.partition()).isEqualTo(1);
      softly.assertThat(record2.offset()).isEqualTo(0);

      ConsumerRecord<Object, Object> record3 = findRecordWithKey(actualRecords, "c");
      softly.assertThat(record3.value()).isEqualTo(expectedValue3);
      softly.assertThat(record3.partition()).isEqualTo(1);
      softly.assertThat(record3.offset()).isEqualTo(1);
    });
  }

  private <K, V> ConsumerRecord<K, V> findRecordWithKey(List<ConsumerRecord<K, V>> records, Object key) {
    Optional<ConsumerRecord<K, V>> recordOptional = records.stream()
      .filter(record -> Objects.equals(record.key(), key))
      .findAny();
    Assertions.assertThat(recordOptional).isPresent();
    return recordOptional.get();
  }

  private Map<String, String> getConfigProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Reference.REFERENCE_NAME, "confluent");
    properties.put(ConfluentStreamingSinkConfig.NAME_BROKERS, KafkaTestUtils.KAFKA_SERVER);
    properties.put(ConfluentStreamingSinkConfig.NAME_TOPIC, topic);
    properties.put(ConfluentStreamingSinkConfig.NAME_ASYNC, "false");
    properties.put(ConfluentStreamingSinkConfig.NAME_COMPRESSION_TYPE, "none");
    properties.put(ConfluentStreamingSinkConfig.NAME_CLUSTER_API_KEY, KafkaTestUtils.CLUSTER_API_KEY);
    properties.put(ConfluentStreamingSinkConfig.NAME_CLUSTER_API_SECRET, KafkaTestUtils.CLUSTER_API_SECRET);
    return properties;
  }

  private SparkManager deploySourcePlugin(ETLPlugin sourcePlugin, Map<String, String> properties) throws Exception {
    return deployETL(
      sourcePlugin,
      new ETLPlugin(ConfluentStreamingSink.PLUGIN_NAME, SparkSink.PLUGIN_TYPE, properties, null),
      "KafkaSinkApp"
    );
  }

  private <K, V> List<ConsumerRecord<K, V>> waitForRecordsInKafka(Consumer<K, V> consumer, int expectedMessages)
    throws Exception {
    List<ConsumerRecord<K, V>> result = new ArrayList<>();
    Stopwatch stopwatch = new Stopwatch();
    while (result.size() < expectedMessages && stopwatch.elapsed(TimeUnit.SECONDS) < 10) {
      ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<K, V> record : records) {
        result.add(record);
      }
    }
    Assertions.assertThat(result).hasSize(expectedMessages);
    return result;
  }
}
