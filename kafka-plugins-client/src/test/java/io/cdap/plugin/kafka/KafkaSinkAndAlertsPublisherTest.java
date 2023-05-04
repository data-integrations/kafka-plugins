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

package io.cdap.plugin.kafka;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.mock.alert.NullAlertTransform;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.kafka.alertpublisher.KafkaAlertPublisher;
import io.cdap.plugin.kafka.sink.KafkaBatchSink;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Kafka Sink and Alerts Publisher test
 */
public class KafkaSinkAndAlertsPublisherTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");

  private static final Gson GSON = new Gson();
  private static final int ZOOKEEPER_TICK_TIME = 1000;
  private static final int ZOOKEEPER_MAX_CONNECTIONS = 100;

  private static ZooKeeperServer zkServer;
  private static ServerCnxnFactory standaloneServerFactory;
  private static EmbeddedKafkaServer kafkaServer;
  private static KafkaConsumer<String, String> kafkaConsumer;
  private int kafkaPort;

  @Before
  public void setupTestClass() throws Exception {
    clear();
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      KafkaBatchSink.class,
                      KafkaAlertPublisher.class,
                      RangeAssignor.class,
                      StringSerializer.class);

    // Initialize zookeeper server
    File zkDir = TMP_FOLDER.newFolder();
    zkServer = new ZooKeeperServer(zkDir, zkDir, ZOOKEEPER_TICK_TIME);
    standaloneServerFactory = ServerCnxnFactory.createFactory(0, ZOOKEEPER_MAX_CONNECTIONS);
    standaloneServerFactory.startup(zkServer);
    String zkConnectionString = String.format("localhost:%d", standaloneServerFactory.getLocalPort());

    // Initialize kafka server
    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkConnectionString, kafkaPort, TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    // Initialize kafka producer
    kafkaConsumer = KafkaTestCommon.getConsumer(kafkaPort);
  }

  @After
  public void cleanup() {
    if (kafkaConsumer != null) {
      kafkaConsumer.close();
    }
    if (kafkaServer != null) {
      kafkaServer.stopAndWait();
    }
    if (standaloneServerFactory != null) {
      standaloneServerFactory.shutdown();
    }
    if (zkServer != null) {
      zkServer.shutdown();
    }
  }

  @Test
  public void testKafkaSinkAndAlertsPublisher() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)));

    // create the pipeline config
    String inputName = "sinkTestInput";

    String usersTopic = "records";
    String alertsTopic = "alerts";
    Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put("brokers", "localhost:" + kafkaPort);
    sinkProperties.put("referenceName", "kafkaTest");
    sinkProperties.put("topic", usersTopic);
    sinkProperties.put("schema", schema.toString());
    sinkProperties.put("format", "csv");
    sinkProperties.put("key", "last");
    sinkProperties.put("async", "FALSE");
    sinkProperties.put("compressionType", "none");

    Map<String, String> alertProperties = new HashMap<>();
    alertProperties.put("brokers", "localhost:" + kafkaPort);
    alertProperties.put("topic", alertsTopic);

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));
    ETLStage sink =
      new ETLStage("sink", new ETLPlugin("Kafka", KafkaBatchSink.PLUGIN_TYPE, sinkProperties, null));
    ETLStage transform = new ETLStage("nullAlert", NullAlertTransform.getPlugin("id"));
    ETLStage alert =
      new ETLStage("alert", new ETLPlugin("KafkaAlerts", KafkaAlertPublisher.PLUGIN_TYPE, alertProperties));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addStage(alert)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .addConnection(transform.getName(), alert.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("testKafkaSink");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));
    
    Set<String> expected = ImmutableSet.of("100,samuel,jackson",
                                           "200,dwayne,johnson",
                                           "300,christopher,walken",
                                           "400,donald,trump");

    List<StructuredRecord> records = new ArrayList<>();
    for (String e : expected) {
      String[] splits = e.split(",");
      StructuredRecord record =
        StructuredRecord.builder(schema)
          .set("id", Long.parseLong(splits[0]))
          .set("first", splits[1])
          .set("last", splits[2])
          .build();
      records.add(record);
    }

    // Add a null record to get an alert
    StructuredRecord nullRecord =
      StructuredRecord.builder(schema)
        .set("first", "terry")
        .set("last", "crews")
        .build();
    records.add(nullRecord);

    DataSetManager<Table> sourceTable = getDataset(inputName);
    MockSource.writeInput(sourceTable, records);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    // Assert users
    Set<String> actual = readKafkaRecords(usersTopic, expected.size());
    Assert.assertEquals(expected, actual);

    // Assert alerts
    Set<String> actualAlerts = readKafkaRecords(alertsTopic, 1);
    // NullAlertTransform always returns empty hash map in alert
    Assert.assertEquals(ImmutableSet.of(new Alert(transform.getName(), new HashMap<String, String>())),
                        ImmutableSet.copyOf(Iterables.transform(actualAlerts,
                                                                new Function<String, Alert>() {
                                                                  @Override
                                                                  public Alert apply(String s) {
                                                                    return GSON.fromJson(s, Alert.class);
                                                                  }
                                                                }
                        )));
  }

  @Test
  public void testKafkaSinkAndAlertsPublisherWithNullKey() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.NULL)));

    // create the pipeline config
    String inputName = "sinkTestInput";

    String usersTopic = "records";
    Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put("brokers", "localhost:" + kafkaPort);
    sinkProperties.put("referenceName", "kafkaTest");
    sinkProperties.put("topic", usersTopic);
    sinkProperties.put("schema", schema.toString());
    sinkProperties.put("format", "csv");
    sinkProperties.put("key", "last");
    sinkProperties.put("async", "FALSE");
    sinkProperties.put("compressionType", "none");

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));
    ETLStage sink =
      new ETLStage("sink", new ETLPlugin("Kafka", KafkaBatchSink.PLUGIN_TYPE, sinkProperties, null));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("testKafkaSink");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));
    
    Set<String> expected = ImmutableSet.of("100,samuel,jackson",
                                           "200,dwayne,johnson",
                                           "300,christopher,walken",
                                           "400,donald,trump");

    List<StructuredRecord> records = new ArrayList<>();
    for (String e : expected) {
      String[] splits = e.split(",");
      StructuredRecord record =
        StructuredRecord.builder(schema)
          .set("id", Long.parseLong(splits[0]))
          .set("first", splits[1])
          .set("last", splits[2])
          .build();
      records.add(record);
    }

    // Add a record with null key
    StructuredRecord recordWithNullKey =
      StructuredRecord.builder(schema)
        .set("id", 500L)
        .set("first", "terry")
        .set("last", null)
        .build();
    records.add(recordWithNullKey);

    DataSetManager<Table> sourceTable = getDataset(inputName);
    MockSource.writeInput(sourceTable, records);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    try {
      workflowManager.start();
      workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);
    } catch (Exception e) {
      Assert.assertTrue(workflowManager.getHistory(ProgramRunStatus.FAILED).size() == 1);
    }
  }

  private Set<String> readKafkaRecords(String topic, final int maxMessages) throws InterruptedException {

    final Set<String> kafkaMessages = new HashSet<>();

    // Subscribe to partition from desired topic and seek to beginning
    List<TopicPartition> partitions = kafkaConsumer
      .partitionsFor(topic)
      .stream()
      .map(partitionInfo ->
             new TopicPartition(topic, partitionInfo.partition()))
      .collect(Collectors.toList());
    kafkaConsumer.assign(partitions);
    kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

    // Poll messages
    int readMessages = 0;
    do {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
      records.iterator().forEachRemaining(r -> kafkaMessages.add(r.value()));
      readMessages += records.count();
    } while (readMessages < maxMessages);
    return kafkaMessages;
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    prop.setProperty("offsets.topic.replication.factor", "1");
    return prop;
  }

}
