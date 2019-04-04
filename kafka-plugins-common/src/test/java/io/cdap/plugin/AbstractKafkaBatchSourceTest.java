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

package io.cdap.plugin;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
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
import io.cdap.plugin.batch.source.KafkaBatchConfig;
import io.cdap.plugin.batch.source.KafkaPartitionOffsets;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.twill.internal.kafka.client.ZKBrokerService;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
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

  private ZKClientService zkClient;
  private KafkaClientService kafkaClient;
  private InMemoryZKServer zkServer;
  private Service kafkaServer;
  private String kafkaBroker;

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
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    for (Class<?> pluginClass : getPluginClasses()) {
      addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"), parentArtifact, pluginClass);
    }

    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    kafkaServer = createKafkaServer(generateKafkaConfig(zkServer.getConnectionStr(), TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();

    BrokerService brokerService = new ZKBrokerService(zkClient);
    brokerService.startAndWait();
    kafkaBroker = brokerService.getBrokerList();
    while (brokerService.getBrokerList().isEmpty()) {
      TimeUnit.MILLISECONDS.sleep(100);
      kafkaBroker = brokerService.getBrokerList();
    }
  }

  @After
  public void cleanup() {
    kafkaClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkClient.stopAndWait();
    zkServer.stopAndWait();
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
    sourceProperties.put("kafkaBrokers", kafkaBroker);
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

  private Properties generateKafkaConfig(String zkConnectStr, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("broker.id", "1");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    return prop;
  }
}
