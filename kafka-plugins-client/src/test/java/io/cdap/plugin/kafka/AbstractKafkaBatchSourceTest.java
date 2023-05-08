/*
 * Copyright © 2016 Cask Data, Inc.
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
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
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
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.kafka.batch.source.KafkaBatchConfig;
import io.cdap.plugin.kafka.batch.source.KafkaPartitionOffsets;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for the Kafka batch source plugins.
 */
public abstract class AbstractKafkaBatchSourceTest extends HydratorTestBase {

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  private static final int ZOOKEEPER_TICK_TIME = 1000;
  private static final int ZOOKEEPER_MAX_CONNECTIONS = 100;

  private static ZooKeeperServer zkServer;
  private static ServerCnxnFactory standaloneServerFactory;
  private static EmbeddedKafkaServer kafkaServer;
  private static KafkaProducer<String, String> kafkaProducer;
  private int kafkaPort;

  /**
   * Creates a {@link Service} for running an embedded Kafka server for testing.
   */
  protected abstract Service createKafkaServer(Properties kafkaConfig);

  /**
   * Returns a {@link List} of plugin classes to be deployed for testing.
   */
  protected abstract List<Class<?>> getPluginClasses();

  /**
   * Returns the name of the Kafka batch source plugin for testing.
   */
  protected abstract String getKafkaBatchSourceName();

  @Before
  public void setup() throws Exception {
    clear();
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    for (Class<?> pluginClass : getPluginClasses()) {
      addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"), parentArtifact, pluginClass);
    }

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
    kafkaProducer = KafkaTestCommon.getProducer(kafkaPort);
  }

  @After
  public void cleanup() {
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
  public void testKafkaSource() throws Exception {
    File offsetDir = TMP_FOLDER.newFolder();

    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)));

    // create the pipeline config
    String outputName = "sourceTestOutput";

    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("kafkaBrokers", "localhost:" + kafkaPort);
    sourceProperties.put("referenceName", "kafkaTest");
    sourceProperties.put("offsetDir", offsetDir.toURI().toString());
    sourceProperties.put("topic", "users");
    sourceProperties.put("schema", schema.toString());
    sourceProperties.put("format", "csv");
    sourceProperties.put("maxNumberRecords", "${maxNumberRecords}");
    ETLStage source = new ETLStage("source", new ETLPlugin(getKafkaBatchSourceName(),
                                                           BatchSource.PLUGIN_TYPE, sourceProperties, null));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputName));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("testKafkaSource");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));


    Map<String, String> messages = new HashMap<>();
    messages.put("a", "1,samuel,jackson");
    messages.put("b", "2,dwayne,johnson");
    messages.put("c", "3,christopher,walken");
    sendKafkaMessage("users", messages);


    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(Collections.singletonMap("maxNumberRecords", "-1"),
                                       ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    // check the pipeline output
    validate(outputName, offsetDir, pipelineId, Collections.singletonMap(0, 3L), ImmutableMap.of(
      1L, "samuel jackson",
      2L, "dwayne johnson",
      3L, "christopher walken"
    ));

    // Validates the Kafka offsets have been preserved
    Path offsetFilePath = KafkaBatchConfig.getOffsetFilePath(new Path(offsetDir.toURI()),
                                                             pipelineId.getNamespace(), pipelineId.getApplication());
    FileContext fc = FileContext.getFileContext(offsetFilePath.toUri());
    KafkaPartitionOffsets partitionOffsets = KafkaPartitionOffsets.load(fc, offsetFilePath);
    Assert.assertEquals(3L, partitionOffsets.getPartitionOffset(0, -1L));

    // Clear the data in the MockSink
    MockSink.clear(getDataset(outputName));

    messages = new LinkedHashMap<>();
    messages.put("d", "4,samuel,jackson");
    messages.put("e", "5,dwayne,johnson");
    messages.put("f", "6,michael,jackson");
    sendKafkaMessage("users", messages);
    workflowManager.startAndWaitForRun(Collections.singletonMap("maxNumberRecords", "2"),
                                       ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    validate(outputName, offsetDir, pipelineId, Collections.singletonMap(0, 5L), ImmutableMap.of(
      4L, "samuel jackson",
      5L, "dwayne johnson"
    ));

    // Clear the data in the MockSink again
    MockSink.clear(getDataset(outputName));

    // Fetch the reminding records
    workflowManager.startAndWaitForRun(Collections.singletonMap("maxNumberRecords", "-1"),
                                       ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    validate(outputName, offsetDir, pipelineId, Collections.singletonMap(0, 6L), Collections.singletonMap(
      6L, "michael jackson"
    ));
  }

  @Test
  public void testKafkaSourceWithInitialPartitionOffsets() throws Exception {

    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)));

    // create the pipeline config
    String outputName = "sourceTestOutput";

    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("kafkaBrokers", "localhost:" + kafkaPort);
    sourceProperties.put("referenceName", "kafkaTest");
    sourceProperties.put("topic", "users");
    sourceProperties.put("schema", schema.toString());
    sourceProperties.put("format", "csv");
    sourceProperties.put("initialPartitionOffsets", "${initialPartitionOffsets}");
    ETLStage source = new ETLStage("source", new ETLPlugin(getKafkaBatchSourceName(),
                                                           BatchSource.PLUGIN_TYPE, sourceProperties, null));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputName));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("testKafkaSource");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    Map<String, String> messages = new HashMap<>();
    messages.put("a", "0,samuel,jackson");
    messages.put("b", "1,dwayne,johnson");
    messages.put("c", "2,christopher,walken");
    messages.put("d", "3,michael,jackson");
    sendKafkaMessage("users", messages);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(Collections.singletonMap("initialPartitionOffsets", "0:2"),
                                       ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputName);
    Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(outputManager));
    Map<Long, String> actual = new HashMap<>();
    for (StructuredRecord outputRecord : outputRecords) {
      actual.put(outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
    }

    Map<Long, String> expected = ImmutableMap.of(
      2L, "christopher walken",
      3L, "michael jackson"
    );
    Assert.assertEquals(expected, actual);
  }

  /**
   * Validates the test result and offsets after the testing pipeline was executed.
   *
   * @param outputName the output mock dataset name
   * @param offsetDir the directory for saving the offset file to
   * @param pipelineId the pipeline Id for testing
   * @param expectedOffsets the expected set of offsets for each partition
   * @param expected the expected results in the output mock dataset
   */
  private void validate(String outputName, File offsetDir,
                        ApplicationId pipelineId,
                        Map<Integer, Long> expectedOffsets,
                        Map<Long, String> expected) throws Exception {
    DataSetManager<Table> outputManager = getDataset(outputName);
    Set<StructuredRecord> outputRecords = new HashSet<>(MockSink.readOutput(outputManager));
    Map<Long, String> actual = new HashMap<>();
    for (StructuredRecord outputRecord : outputRecords) {
      actual.put(outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
    }

    Assert.assertEquals(expected, actual);

    // Validates the offsets
    Path offsetFilePath = KafkaBatchConfig.getOffsetFilePath(new Path(offsetDir.toURI()),
                                                             pipelineId.getNamespace(), pipelineId.getApplication());
    FileContext fc = FileContext.getFileContext(offsetFilePath.toUri());
    KafkaPartitionOffsets partitionOffsets = KafkaPartitionOffsets.load(fc, offsetFilePath);

    expectedOffsets.forEach((partition, offset) ->
                              Assert.assertEquals(offset.longValue(),
                                                  partitionOffsets.getPartitionOffset(partition, -1L)));
  }

  private Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("broker.id", "1");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("zookeeper.session.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    prop.setProperty("offsets.topic.replication.factor", "1");
    return prop;
  }

  private void sendKafkaMessage(@SuppressWarnings("SameParameterValue") String topic, Map<String, String> messages) {
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      kafkaProducer.send(new ProducerRecord<>(topic, entry.getKey(), entry.getValue()));
    }
  }
}
