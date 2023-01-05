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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.common.http.HTTPPollConfig;
import io.cdap.plugin.kafka.source.KafkaStreamingSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.cdap.plugin.kafka.source.KafkaConfig.OFFSET_START_FROM_BEGINNING;

/**
 * Tests for Spark plugins.
 */
public class KafkaStreamingSourceBegginingOffsetTest extends HydratorTestBase {

  // Explicitly turn off state tracking to ensure checkpointing is on.
  // This test needs a fix to work with checkpointing disabled. See PLUGIN-1414
  @ClassRule
  public static final TestConfiguration CONFIG =
    new TestConfiguration("feature.streaming.pipeline.native.state.tracking.enabled", "false");

  private static final ArtifactId DATAPIPELINE_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", "4.3.2");
  private static final ArtifactId DATASTREAMS_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-streams", "4.3.2");
  private static final ArtifactSummary DATASTREAMS_ARTIFACT =
    new ArtifactSummary("data-streams", "4.3.2");
  private static final int ZOOKEEPER_TICK_TIME = 1000;
  private static final int ZOOKEEPER_MAX_CONNECTIONS = 100;

  private static ZooKeeperServer zkServer;
  private static ServerCnxnFactory standaloneServerFactory;
  private static EmbeddedKafkaServer kafkaServer;
  private static KafkaProducer<String, String> kafkaProducer;
  private static int kafkaPort;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for data pipeline app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    setupStreamingArtifacts(DATASTREAMS_ARTIFACT_ID, DataStreamsApp.class);

    // add artifact for spark plugins
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true),
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATASTREAMS_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), parents,
                      KafkaStreamingSource.class, KafkaUtils.class, Deserializer.class, ByteArrayDeserializer.class,
                      TopicPartition.class, HTTPPollConfig.class);

    // Initialize zookeeper server
    File zkDir = TMP_FOLDER.newFolder();
    zkServer = new ZooKeeperServer(zkDir, zkDir, ZOOKEEPER_TICK_TIME);
    standaloneServerFactory = ServerCnxnFactory.createFactory(0, ZOOKEEPER_MAX_CONNECTIONS);
    standaloneServerFactory.startup(zkServer);
    String zkConnectionString = String.format("localhost:%d", standaloneServerFactory.getLocalPort());

    // Initialize kafka server
    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(KafkaTestCommon.generateKafkaConfig(zkConnectionString,
                                                                              kafkaPort,
                                                                              TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    // Initialize kafka producer
    kafkaProducer = KafkaTestCommon.getProducer(kafkaPort);
  }

  @AfterClass
  public static void cleanup() {
    if (kafkaProducer != null) {
      kafkaProducer.close();
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
  public void testKafkaStreamingSourceFromBeginningOffset() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)));

    Map<String, String> properties = new HashMap<>();
    properties.put("referenceName", "kafkaPurchasesBeginningOffsetTest");
    properties.put("brokers", "localhost:" + kafkaPort);
    properties.put("topic", "usersWithBeginningOffset");
    properties.put("initialOffset", OFFSET_START_FROM_BEGINNING);
    properties.put("format", "csv");
    properties.put("schema", schema.toString());

    ETLStage source = new ETLStage("source", new ETLPlugin("Kafka", StreamingSource.PLUGIN_TYPE, properties, null));

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(new ETLStage("sink", MockSink.getPlugin("kafkaOutputBeginningOffsetTest")))
      .addConnection("source", "sink")
      .setBatchInterval("1s")
      .setStopGracefully(true)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(DATASTREAMS_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("KafkaSourceApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write some messages to kafka
    Map<String, String> messages = new HashMap<>();
    messages.put("a", "1,samuel,jackson");
    messages.put("b", "2,dwayne,johnson");
    messages.put("c", "3,christopher,walken");
    KafkaTestCommon.sendKafkaMessage(kafkaProducer, "usersWithBeginningOffset", messages);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    final DataSetManager<Table> outputManager = getDataset("kafkaOutputBeginningOffsetTest");
    Tasks.waitFor(
      ImmutableMap.of(1L, "samuel jackson", 2L, "dwayne johnson", 3L, "christopher walken"),
      () -> {
        outputManager.flush();
        Map<Long, String> actual = new HashMap<>();
        for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
          actual.put(outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
        }
        return actual;
      }, 2, TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStatus(false, 10, 1);

    // clear the output table
    MockSink.clear(outputManager);

    // now write some more messages to kafka and start the program again to make sure it picks up where it left off
    messages = new HashMap<>();
    messages.put("d", "4,terry,crews");
    messages.put("e", "5,sylvester,stallone");
    KafkaTestCommon.sendKafkaMessage(kafkaProducer, "usersWithBeginningOffset", messages);

    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    Tasks.waitFor(
      ImmutableMap.of(4L, "terry crews", 5L, "sylvester stallone"),
      () -> {
        outputManager.flush();
        Map<Long, String> actual = new HashMap<>();
        for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
          actual.put(outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
        }
        return actual;
      }, 2, TimeUnit.MINUTES);

    sparkManager.stop();
  }
}
