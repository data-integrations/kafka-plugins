/*
 * Copyright © 2022 Cask Data, Inc.
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
import com.google.gson.Gson;
import io.cdap.cdap.api.app.AppStateStore;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.transform.RecoveringTransform;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestBase;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Kafka streaming source with state store with failure
 * One test case per test because Kafka keeps static references to some classes.
 * See https://cdap.atlassian.net/browse/PLUGIN-1414 for details.
 */
public class KafkaStreamingSourceStateStoreFailureTest extends HydratorTestBase {

  // Turn on state tracking.
  @ClassRule
  public static final TestConfiguration CONFIG =
    new TestConfiguration("explore.enabled", false,
                          "feature.streaming.pipeline.native.state.tracking.enabled", "true");
  private static final Gson GSON = new Gson();

  private static final ArtifactId DATASTREAMS_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-streams", "6.8.0");
  private static final ArtifactSummary DATASTREAMS_ARTIFACT =
    new ArtifactSummary("data-streams", "6.8.0");
  private static final int ZOOKEEPER_TICK_TIME = 1000;
  private static final int ZOOKEEPER_MAX_CONNECTIONS = 100;
  private static final String SOURCE_REFERENCE_NAME = "kafkaPurchases";
  private static final String SRC_STAGE_NAME = "source123";
  private static final String TOPIC_NAME = "users";

  private static ZooKeeperServer zkServer;
  private static ServerCnxnFactory standaloneServerFactory;
  private static EmbeddedKafkaServer kafkaServer;
  private static KafkaProducer<String, String> kafkaProducer;
  private static int kafkaPort;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupTest() throws Exception {
    setupStreamingArtifacts(DATASTREAMS_ARTIFACT_ID, DataStreamsApp.class);

    // add artifact for kafka plugins
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATASTREAMS_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("kafka-plugins", "1.0.0"), parents,
                      KafkaStreamingSource.class, KafkaUtils.class, Deserializer.class,
                      ByteArrayDeserializer.class,
                      TopicPartition.class, HTTPPollConfig.class);

    // Initialize zookeeper server
    File zkDir = TMP_FOLDER.newFolder();
    zkServer = new ZooKeeperServer(zkDir, zkDir, ZOOKEEPER_TICK_TIME);
    standaloneServerFactory = ServerCnxnFactory.createFactory(0, ZOOKEEPER_MAX_CONNECTIONS);
    standaloneServerFactory.startup(zkServer);
    String zkConnectionString = String.format("localhost:%d",
                                              standaloneServerFactory.getLocalPort());

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
    RecoveringTransform.reset();
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
  public void testKafkaStreamingSource() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)));

    Map<String, String> properties = new HashMap<>();
    properties.put("referenceName", SOURCE_REFERENCE_NAME);
    properties.put("brokers", "localhost:" + kafkaPort);
    properties.put("topic", TOPIC_NAME);
    properties.put("defaultInitialOffset", "-2");
    properties.put("format", "csv");
    properties.put("schema", schema.toString());

    ETLStage source = new ETLStage(SRC_STAGE_NAME,
                                   new ETLPlugin("Kafka", StreamingSource.PLUGIN_TYPE, properties, null));

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(new ETLStage("retry_transform", RecoveringTransform.getPlugin()))
      .addStage(new ETLStage("sink", MockSink.getPlugin("kafkaOutput")))
      .addConnection(SRC_STAGE_NAME, "retry_transform")
      .addConnection("retry_transform", "sink")
      .setBatchInterval("1s")
      .setStopGracefully(false)
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(DATASTREAMS_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("KafkaSourceApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write some messages to kafka
    Map<String, String> messages = new HashMap<>();
    messages.put("a", "1,samuel,jackson");
    messages.put("b", "2,dwayne,johnson");
    messages.put("c", "5,christopher,walken");
    KafkaTestCommon.sendKafkaMessage(kafkaProducer, TOPIC_NAME, messages);

    // Launch the program with runtime args for timeout
    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start(ImmutableMap.of(
      "cdap.streaming.maxRetryTimeInMins", "1",
      "cdap.streaming.baseRetryDelayInSeconds", "60"));
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 2, TimeUnit.MINUTES);

    // Should fail after retry times out
    sparkManager.waitForRun(ProgramRunStatus.FAILED, 2, TimeUnit.MINUTES);

    // Verify that state is not saved.
    AppStateStore appStateStore = TestBase.getAppStateStore(appId.getNamespace(),
                                                            appId.getApplication());
    Optional<byte[]> savedState = appStateStore.getState(SRC_STAGE_NAME + "." + TOPIC_NAME);
    Assert.assertFalse(savedState.isPresent());
  }
}
