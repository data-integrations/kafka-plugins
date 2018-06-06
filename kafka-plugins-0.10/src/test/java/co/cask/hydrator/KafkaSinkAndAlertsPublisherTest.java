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

package co.cask.hydrator;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Alert;
import co.cask.cdap.etl.mock.alert.NullAlertTransform;
import co.cask.cdap.etl.mock.batch.MockSource;
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
import co.cask.hydrator.plugin.alertpublisher.KafkaAlertPublisher;
import co.cask.hydrator.plugin.sink.KafkaBatchSink;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Sink and Alerts Publisher test
 */
public class KafkaSinkAndAlertsPublisherTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final Gson GSON = new Gson();

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
                      KafkaBatchSink.class,
                      KafkaAlertPublisher.class,
                      RangeAssignor.class,
                      StringSerializer.class);

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

  @AfterClass
  public static void cleanup() {
    kafkaClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkClient.stopAndWait();
    zkServer.stopAndWait();
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

  private Set<String> readKafkaRecords(String topic, final int maxMessages) throws InterruptedException {
    KafkaConsumer kafkaConsumer = kafkaClient.getConsumer();

    final Set<String> kafkaMessages = new HashSet<>();
    KafkaConsumer.Preparer preparer = kafkaConsumer.prepare();
    preparer.addFromBeginning(topic, 0);

    final CountDownLatch stopLatch = new CountDownLatch(1);
    Cancellable cancellable = preparer.consume(new KafkaConsumer.MessageCallback() {
      @Override
      public long onReceived(Iterator<FetchedMessage> messages) {
        long nextOffset = 0;
        while (messages.hasNext()) {
          FetchedMessage message = messages.next();
          nextOffset = message.getNextOffset();
          String payload = Charsets.UTF_8.decode(message.getPayload()).toString();
          kafkaMessages.add(payload);
        }
        // We are done when maxMessages are received
        if (kafkaMessages.size() >= maxMessages) {
          stopLatch.countDown();
        }
        return nextOffset;
      }

      @Override
      public void finished() {
        // nothing to do
      }
    });

    stopLatch.await(30, TimeUnit.SECONDS);
    cancellable.cancel();
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
    return prop;
  }

}
