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

package io.cdap.plugin.confluent.integration.streaming.source;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.confluent.integration.KafkaTestUtils;
import io.cdap.plugin.confluent.integration.streaming.ConfluentStreamingTestBase;
import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSource;
import io.cdap.plugin.confluent.streaming.source.ConfluentStreamingSourceConfig;
import io.cdap.plugin.format.avro.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Tests for Confluent Streaming Source plugin.
 */
public class ConfluentStreamingSourceTest extends ConfluentStreamingTestBase {

  private static KafkaProducer<byte[], byte[]> kafkaProducer;
  private static KafkaProducer<Object, Object> kafkaAvroProducer;

  @Rule
  public TestName testName = new TestName();

  private String topic;
  private String outputTable;
  private SparkManager programManager;

  @BeforeClass
  public static void setupTestClass() {
    kafkaProducer = KafkaTestUtils.createProducer();
    kafkaAvroProducer = KafkaTestUtils.createProducerForSchemaRegistry();
  }

  @AfterClass
  public static void cleanupTestClass() {
    kafkaProducer.close();
    kafkaAvroProducer.close();
  }

  @Before
  public void setUp() {
    outputTable = testName.getMethodName() + "_out";
    topic = ConfluentStreamingSourceTest.class.getSimpleName() + "_" + testName.getMethodName();
    KafkaTestUtils.deleteTopic(topic);
    KafkaTestUtils.createTopic(topic, 2, 3);
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
  public void testConfluentStreamingSource() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING))
    );
    Map<String, String> properties = getConfigProperties(schema);
    properties.put(ConfluentStreamingSourceConfig.NAME_FORMAT, "csv");
    programManager = deploySourcePlugin(properties);
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    sendKafkaMessage(topic, 0, "a", "1,samuel,jackson");

    List<StructuredRecord> expectedRecords = Collections.singletonList(
      StructuredRecord.builder(schema)
        .set("id", 1L)
        .set("first", "samuel")
        .set("last", "jackson")
        .build()
    );
    waitForRecords(outputTable, expectedRecords);

    programManager.stop();
    programManager.waitForStopped(10, TimeUnit.SECONDS);

    // clear the output table
    DataSetManager<Table> outputManager = getDataset(outputTable);
    MockSink.clear(outputManager);

    // now write some more messages to kafka and start the program again to make sure it picks up where it left off
    sendKafkaMessage(topic, 1, "b", "2,dwayne,johnson");

    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    List<StructuredRecord> expectedRecords2 = Collections.singletonList(
      StructuredRecord.builder(schema)
        .set("id", 2L)
        .set("first", "dwayne")
        .set("last", "johnson")
        .build()
    );
    waitForRecords(outputTable, expectedRecords2);
  }

  @Test
  public void testConfluentStreamingSourceAdditionalFieldsWithoutFormat() throws Exception {
    String keyField = "key";
    String timeField = "time";
    String partitionField = "partition";
    String offsetField = "offset";
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of(keyField, Schema.of(Schema.Type.BYTES)),
      Schema.Field.of(timeField, Schema.of(Schema.Type.LONG)),
      Schema.Field.of(partitionField, Schema.of(Schema.Type.INT)),
      Schema.Field.of(offsetField, Schema.of(Schema.Type.LONG))
    );
    Map<String, String> properties = getConfigProperties(schema);
    properties.put(ConfluentStreamingSourceConfig.NAME_KEYFIELD, keyField);
    properties.put(ConfluentStreamingSourceConfig.NAME_TIMEFIELD, timeField);
    properties.put(ConfluentStreamingSourceConfig.NAME_PARTITION_FIELD, partitionField);
    properties.put(ConfluentStreamingSourceConfig.NAME_OFFSET_FIELD, offsetField);
    programManager = deploySourcePlugin(properties);
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    sendKafkaMessage(topic, 1, "a", "payload_1");
    sendKafkaMessage(topic, 0, "b", "payload_2");
    sendKafkaMessage(topic, 0, "c", "payload_3");

    List<StructuredRecord> actualRecords = waitForRecords(outputTable, 3);

    SoftAssertions.assertSoftly(softly -> {
      StructuredRecord output1 =
        findRecordWithField(actualRecords, keyField, ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output1.<Object>get("message"))
        .isEqualTo(ByteBuffer.wrap("payload_1".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output1.<Object>get(keyField))
        .isEqualTo(ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output1.<Long>get(timeField)).isPositive();
      softly.assertThat(output1.<Integer>get(partitionField)).isEqualTo(1);
      softly.assertThat(output1.<Long>get(offsetField)).isEqualTo(0);

      StructuredRecord output2 =
        findRecordWithField(actualRecords, keyField, ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output2.<Object>get("message"))
        .isEqualTo(ByteBuffer.wrap("payload_2".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output2.<Object>get(keyField))
        .isEqualTo(ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output2.<Long>get(timeField)).isPositive();
      softly.assertThat(output2.<Integer>get(partitionField)).isEqualTo(0);
      softly.assertThat(output2.<Long>get(offsetField)).isEqualTo(0);

      StructuredRecord output3 =
        findRecordWithField(actualRecords, keyField, ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output3.<Object>get("message"))
        .isEqualTo(ByteBuffer.wrap("payload_3".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output3.<Object>get(keyField))
        .isEqualTo(ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output3.<Long>get(timeField)).isPositive();
      softly.assertThat(output3.<Integer>get(partitionField)).isEqualTo(0);
      softly.assertThat(output3.<Long>get(offsetField)).isEqualTo(1);
    });
  }

  @Test
  public void testConfluentStreamingSourceAdditionalFieldsWithFormat() throws Exception {
    String keyField = "key";
    String timeField = "time";
    String partitionField = "partition";
    String offsetField = "offset";
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(keyField, Schema.of(Schema.Type.BYTES)),
      Schema.Field.of(timeField, Schema.of(Schema.Type.LONG)),
      Schema.Field.of(partitionField, Schema.of(Schema.Type.INT)),
      Schema.Field.of(offsetField, Schema.of(Schema.Type.LONG))
    );
    Map<String, String> properties = getConfigProperties(schema);
    properties.put(ConfluentStreamingSourceConfig.NAME_FORMAT, "csv");
    properties.put(ConfluentStreamingSourceConfig.NAME_KEYFIELD, keyField);
    properties.put(ConfluentStreamingSourceConfig.NAME_TIMEFIELD, timeField);
    properties.put(ConfluentStreamingSourceConfig.NAME_PARTITION_FIELD, partitionField);
    properties.put(ConfluentStreamingSourceConfig.NAME_OFFSET_FIELD, offsetField);
    programManager = deploySourcePlugin(properties);
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    sendKafkaMessage(topic, 1, "a", "1,samuel,jackson");
    sendKafkaMessage(topic, 0, "b", "2,dwayne,johnson");
    sendKafkaMessage(topic, 0, "c", "3,christopher,walken");

    List<StructuredRecord> actualRecords = waitForRecords(outputTable, 3);

    SoftAssertions.assertSoftly(softly -> {
      StructuredRecord output1 = findRecordWithField(actualRecords, "id", 1L);
      softly.assertThat(output1.<Object>get("id")).isEqualTo(1L);
      softly.assertThat(output1.<Object>get("first")).isEqualTo("samuel");
      softly.assertThat(output1.<Object>get("last")).isEqualTo("jackson");
      softly.assertThat(output1.<Object>get(keyField))
        .isEqualTo(ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output1.<Long>get(timeField)).isPositive();
      softly.assertThat(output1.<Integer>get(partitionField)).isEqualTo(1);
      softly.assertThat(output1.<Long>get(offsetField)).isEqualTo(0);

      StructuredRecord output2 = findRecordWithField(actualRecords, "id", 2L);
      softly.assertThat(output2.<Object>get("id")).isEqualTo(2L);
      softly.assertThat(output2.<Object>get("first")).isEqualTo("dwayne");
      softly.assertThat(output2.<Object>get("last")).isEqualTo("johnson");
      softly.assertThat(output2.<Object>get(keyField))
        .isEqualTo(ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output2.<Long>get(timeField)).isPositive();
      softly.assertThat(output2.<Integer>get(partitionField)).isEqualTo(0);
      softly.assertThat(output2.<Long>get(offsetField)).isEqualTo(0);

      StructuredRecord output3 = findRecordWithField(actualRecords, "id", 3L);
      softly.assertThat(output3.<Object>get("id")).isEqualTo(3L);
      softly.assertThat(output3.<Object>get("first")).isEqualTo("christopher");
      softly.assertThat(output3.<Object>get("last")).isEqualTo("walken");
      softly.assertThat(output3.<Object>get(keyField))
        .isEqualTo(ByteBuffer.wrap("c".getBytes(StandardCharsets.UTF_8)));
      softly.assertThat(output3.<Long>get(timeField)).isPositive();
      softly.assertThat(output3.<Integer>get(partitionField)).isEqualTo(0);
      softly.assertThat(output3.<Long>get(offsetField)).isEqualTo(1);
    });
  }

  @Test
  public void testConfluentStreamingSourceAdditionalFieldsWithSchemaRegistry() throws Exception {
    String messageField = "message";
    String keyField = "key";
    String timeField = "time";
    String partitionField = "partition";
    String offsetField = "offset";
    Schema schema = Schema.recordOf(
      "confluent",
      Schema.Field.of(timeField, Schema.of(Schema.Type.LONG)),
      Schema.Field.of(partitionField, Schema.of(Schema.Type.INT)),
      Schema.Field.of(offsetField, Schema.of(Schema.Type.LONG))
    );
    Map<String, String> properties = getConfigProperties(schema);
    properties.put(ConfluentStreamingSourceConfig.NAME_SR_URL, KafkaTestUtils.SR_URL);
    properties.put(ConfluentStreamingSourceConfig.NAME_SR_API_KEY, KafkaTestUtils.SR_API_KEY);
    properties.put(ConfluentStreamingSourceConfig.NAME_SR_API_SECRET, KafkaTestUtils.SR_API_SECRET);
    properties.put(ConfluentStreamingSourceConfig.NAME_VALUE_FIELD, messageField);
    properties.put(ConfluentStreamingSourceConfig.NAME_KEYFIELD, keyField);
    properties.put(ConfluentStreamingSourceConfig.NAME_TIMEFIELD, timeField);
    properties.put(ConfluentStreamingSourceConfig.NAME_PARTITION_FIELD, partitionField);
    properties.put(ConfluentStreamingSourceConfig.NAME_OFFSET_FIELD, offsetField);
    programManager = deploySourcePlugin(properties);
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    Schema inputSchema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING))
    );
    StructuredRecord inputRecord1 = StructuredRecord.builder(inputSchema)
      .set("id", 1L)
      .set("first", "samuel")
      .set("last", "jackson")
      .build();
    StructuredRecord inputRecord2 = StructuredRecord.builder(inputSchema)
      .set("id", 2L)
      .set("first", "dwayne")
      .set("last", "johnson")
      .build();
    StructuredRecord inputRecord3 = StructuredRecord.builder(inputSchema)
      .set("id", 3L)
      .set("first", "christopher")
      .set("last", "walken")
      .build();
    StructuredToAvroTransformer transformer = new StructuredToAvroTransformer(inputSchema);
    sendKafkaAvroMessage(topic, 1, "a", transformer.transform(inputRecord1));
    sendKafkaAvroMessage(topic, 0, "b", transformer.transform(inputRecord2));
    sendKafkaAvroMessage(topic, 0, "c", transformer.transform(inputRecord3));

    List<StructuredRecord> actualRecords = waitForRecords(outputTable, 3);

    SoftAssertions.assertSoftly(softly -> {
      StructuredRecord output1 = findRecordWithField(actualRecords, keyField, "a");
      softly.assertThat(output1.<Object>get(messageField)).isEqualTo(inputRecord1);
      softly.assertThat(output1.<Object>get(keyField)).isEqualTo("a");
      softly.assertThat(output1.<Long>get(timeField)).isPositive();
      softly.assertThat(output1.<Integer>get(partitionField)).isEqualTo(1);
      softly.assertThat(output1.<Long>get(offsetField)).isEqualTo(0);

      StructuredRecord output2 = findRecordWithField(actualRecords, keyField, "b");
      softly.assertThat(output2.<Object>get(messageField)).isEqualTo(inputRecord2);
      softly.assertThat(output2.<Object>get(keyField)).isEqualTo("b");
      softly.assertThat(output2.<Long>get(timeField)).isPositive();
      softly.assertThat(output2.<Integer>get(partitionField)).isEqualTo(0);
      softly.assertThat(output2.<Long>get(offsetField)).isEqualTo(0);

      StructuredRecord output3 = findRecordWithField(actualRecords, keyField, "c");
      softly.assertThat(output3.<Object>get(messageField)).isEqualTo(inputRecord3);
      softly.assertThat(output3.<Object>get(keyField)).isEqualTo("c");
      softly.assertThat(output3.<Long>get(timeField)).isPositive();
      softly.assertThat(output3.<Integer>get(partitionField)).isEqualTo(0);
      softly.assertThat(output3.<Long>get(offsetField)).isEqualTo(1);
    });
  }

  @Test
  public void testConfluentStreamingSourceWithSchemaRegistry() throws Exception {
    String messageField = "message";
    Schema valueSchema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING))
    );
    Schema schema = Schema.recordOf(
      "confluent",
      Schema.Field.of(messageField, valueSchema)
    );
    Map<String, String> properties = getConfigProperties(schema);
    properties.put(ConfluentStreamingSourceConfig.NAME_SR_URL, KafkaTestUtils.SR_URL);
    properties.put(ConfluentStreamingSourceConfig.NAME_SR_API_KEY, KafkaTestUtils.SR_API_KEY);
    properties.put(ConfluentStreamingSourceConfig.NAME_SR_API_SECRET, KafkaTestUtils.SR_API_SECRET);
    properties.put(ConfluentStreamingSourceConfig.NAME_VALUE_FIELD, messageField);
    programManager = deploySourcePlugin(properties);
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    List<StructuredRecord> records = Arrays.asList(
      StructuredRecord.builder(valueSchema)
        .set("id", 1L)
        .set("first", "samuel")
        .set("last", "jackson")
        .build(),
      StructuredRecord.builder(valueSchema)
        .set("id", 2L)
        .set("first", "dwayne")
        .set("last", "johnson")
        .build(),
      StructuredRecord.builder(valueSchema)
        .set("id", 3L)
        .set("first", "christopher")
        .set("last", "walken")
        .build()
    );
    List<GenericRecord> genericRecords = KafkaTestUtils.toGenericRecords(records, valueSchema);
    for (GenericRecord genericRecord : genericRecords) {
      sendKafkaAvroMessage(topic, 0, null, genericRecord);
    }

    List<StructuredRecord> expectedRecords = records.stream()
      .map(record -> StructuredRecord.builder(schema)
        .set(messageField, record)
        .build())
      .collect(Collectors.toList());
    waitForRecords(outputTable, expectedRecords);
  }

  private StructuredRecord findRecordWithField(List<StructuredRecord> records, String fieldName, Object value) {
    Optional<StructuredRecord> recordOptional = records.stream()
      .filter(record -> Objects.equals(record.get(fieldName), value))
      .findAny();
    Assertions.assertThat(recordOptional).isPresent();
    return recordOptional.get();
  }

  private Map<String, String> getConfigProperties(Schema schema) {
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Reference.REFERENCE_NAME, "confluent");
    properties.put(ConfluentStreamingSourceConfig.NAME_BROKERS, KafkaTestUtils.KAFKA_SERVER);
    properties.put(ConfluentStreamingSourceConfig.NAME_TOPIC, topic);
    properties.put(ConfluentStreamingSourceConfig.NAME_DEFAULT_INITIAL_OFFSET,
                   String.valueOf(ListOffsetRequest.EARLIEST_TIMESTAMP));
    properties.put(ConfluentStreamingSourceConfig.NAME_CLUSTER_API_KEY, KafkaTestUtils.CLUSTER_API_KEY);
    properties.put(ConfluentStreamingSourceConfig.NAME_CLUSTER_API_SECRET, KafkaTestUtils.CLUSTER_API_SECRET);
    properties.put(ConfluentStreamingSourceConfig.NAME_SCHEMA, schema.toString());
    properties.put(ConfluentStreamingSourceConfig.NAME_MAX_RATE, "1000");
    return properties;
  }

  private SparkManager deploySourcePlugin(Map<String, String> properties) throws Exception {
    return deployETL(
      new ETLPlugin(ConfluentStreamingSource.PLUGIN_NAME, StreamingSource.PLUGIN_TYPE, properties, null),
      MockSink.getPlugin(outputTable),
      "KafkaSourceApp"
    );
  }

  private void sendKafkaAvroMessage(String topic, @Nullable Integer partition, @Nullable Object key, Object value) {
    try {
      kafkaAvroProducer.send(new ProducerRecord<>(topic, partition, key, value)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  private void sendKafkaMessage(String topic, @Nullable Integer partition, @Nullable String key, String value) {
    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
    byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
    try {
      kafkaProducer.send(new ProducerRecord<>(topic, partition, keyBytes, valueBytes)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }
}
