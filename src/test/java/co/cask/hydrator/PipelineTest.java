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

package co.cask.hydrator;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.batchSource.KafkaBatchSource;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import kafka.common.TopicAndPartition;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;
  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static int kafkaPort;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      KafkaBatchSource.class);

    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkServer.getConnectionStr(),
                                                              kafkaPort, TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();
  }

  @Test
  public void testKafkaSource() throws Exception {
    Schema schema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)));

    // create the pipeline config
    String inputName = "sourceTestInput";
    String outputName = "sourceTestOutput";

    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("kafkaBrokers", "localhost:" + kafkaPort);
    sourceProperties.put("referenceName", "kafkaTest");
    sourceProperties.put("tableName", "testKafkaSource");
    sourceProperties.put("topic", "users");
    sourceProperties.put("schema", schema.toString());
    sourceProperties.put("format", "csv");
    ETLStage source =
      new ETLStage("source", new ETLPlugin(KafkaBatchSource.NAME, BatchSource.PLUGIN_TYPE, sourceProperties, null));
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputName));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
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
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    // check the pipeline output
    DataSetManager<Table> outputManager = getDataset(outputName);
    Set<StructuredRecord> outputRecords = new HashSet<>();
    outputRecords.addAll(MockSink.readOutput(outputManager));

    final Map<Long, String> expected = ImmutableMap.of(
      1L, "samuel jackson",
      2L, "dwayne johnson",
      3L, "christopher walken"
    );

    Map<Long, String> actual = new HashMap<>();
    for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
      actual.put((Long) outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
    }

    Assert.assertEquals(3, outputRecords.size());
    Assert.assertEquals(expected, actual);
    DataSetManager<KeyValueTable> kvTable = getDataset("testKafkaSource");
    KeyValueTable table = kvTable.get();
    byte[] offset = table.read(new TopicAndPartition("users", 0).toString());
    Assert.assertNotNull(offset);
    Assert.assertEquals(3, Bytes.toLong(offset));

    messages = new HashMap<>();
    messages.put("d", "4,samuel,jackson");
    messages.put("e", "5,dwayne,johnson");
    sendKafkaMessage("users", messages);
    workflowManager.start();
    TimeUnit.SECONDS.sleep(10);
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);
    final Map<Long, String> expected2 = ImmutableMap.of(
      1L, "samuel jackson",
      2L, "dwayne johnson",
      3L, "christopher walken",
      4L, "samuel jackson",
      5L, "dwayne johnson"
    );

    outputRecords = new HashSet<>();
    outputRecords.addAll(MockSink.readOutput(outputManager));
    Map<Long, String> actual2 = new HashMap<>();
    for (StructuredRecord outputRecord : MockSink.readOutput(outputManager)) {
      actual2.put((Long) outputRecord.get("id"), outputRecord.get("first") + " " + outputRecord.get("last"));
    }
    Assert.assertEquals(5, outputRecords.size());
    Assert.assertEquals(expected2, actual2);
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
    return prop;
  }

  private void sendKafkaMessage(String topic, Map<String, String> messages) {
    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, Compression.NONE);

    // If publish failed, retry up to 20 times, with 100ms delay between each retry
    // This is because leader election in Kafka 08 takes time when a topic is being created upon publish request.
    int count = 0;
    do {
      KafkaPublisher.Preparer preparer = publisher.prepare(topic);
      for (Map.Entry<String, String> entry : messages.entrySet()) {
        preparer.add(Charsets.UTF_8.encode(entry.getValue()), entry.getKey());
      }
      try {
        preparer.send().get();
        break;
      } catch (Exception e) {
        // Backoff if send failed.
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    } while (count++ < 20);
  }
}
