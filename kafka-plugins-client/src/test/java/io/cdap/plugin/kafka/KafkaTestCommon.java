/*
 * Copyright © 2025 Cask Data, Inc.
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

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.common.utils.Tasks;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common methods for all tests
 */
public class KafkaTestCommon {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTestCommon.class);
  private static final long KAFKA_SERVER_TIMEOUT_SECONDS = 30L;
  private static final Pattern CHECKPOINT_FILE_PATTERN = Pattern.compile("^checkpoint-(\\d+)$");
  private static final Pattern CHECKPOINT_BACKUP_PATTERN = Pattern.compile("^checkpoint-(\\d+)\\.bk$");

  public static void startKafkaServer(Service kafkaServer) throws TimeoutException {
    kafkaServer.startAsync().awaitRunning(KAFKA_SERVER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  public static void stopKafkaServer(Service kafkaServer) {
    if (kafkaServer == null) {
      return;
    }
    try {
      kafkaServer.stopAsync().awaitTerminated(KAFKA_SERVER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (IllegalStateException | TimeoutException e) {
      LOG.warn("Failed to stop embedded Kafka server cleanly", e);
    }
  }

  public static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("zookeeper.session.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    prop.setProperty("offsets.topic.replication.factor", "1");
    return prop;
  }

  public static KafkaConsumer<String, String> getConsumer(int kafkaPort) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringDeserializer");
    return new KafkaConsumer<>(props);
  }

  public static KafkaProducer<String, String> getProducer(int kafkaPort) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer");
    return new KafkaProducer<>(props);
  }

  public static void sendKafkaMessage(KafkaProducer kafkaProducer,
                                      String topic,
                                      Map<String, String> messages) {
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      kafkaProducer.send(new ProducerRecord<>(topic, entry.getKey(), entry.getValue()));
    }
    kafkaProducer.flush();
  }

  /**
   * Waits until Spark Streaming has written a post-batch-completion checkpoint for a batch strictly
   * after {@code afterTimeMillis}. In CDAP unit tests, {@code ProgramLifecycleService.stop} stops
   * Spark with a 0s graceful timeout, causing {@code SparkStreamingPipelineDriver} to stop the
   * streaming context immediately without waiting for pending batches to finish checkpointing.
   * Waiting for both {@code checkpoint-<ts>} and {@code checkpoint-<ts>.bk} ensures a batch strictly
   * after {@code afterTimeMillis} has completed and written its post-completion checkpoint.
   */
  public static void waitForCheckpointAfter(Callable<FileSet> checkpointDatasetSupplier,
                                            String appName,
                                            long afterTimeMillis) throws Exception {
    Tasks.waitFor(true, () -> {
      FileSet checkpointDataset = checkpointDatasetSupplier.call();
      if (checkpointDataset == null) {
        return false;
      }
      Location appDir = checkpointDataset.getBaseLocation().append(appName);
      if (!appDir.exists() || !appDir.isDirectory()) {
        return false;
      }
      for (Location pipelineDir : appDir.list()) {
        if (!pipelineDir.isDirectory()) {
          continue;
        }
        Set<Long> checkpointTimes = new HashSet<>();
        Set<Long> backupTimes = new HashSet<>();
        try {
          for (Location file : pipelineDir.list()) {
            String name = file.getName();
            Matcher checkpointMatcher = CHECKPOINT_FILE_PATTERN.matcher(name);
            if (checkpointMatcher.matches() && file.exists() && file.length() > 0) {
              checkpointTimes.add(Long.parseLong(checkpointMatcher.group(1)));
              continue;
            }
            Matcher backupMatcher = CHECKPOINT_BACKUP_PATTERN.matcher(name);
            if (backupMatcher.matches()) {
              backupTimes.add(Long.parseLong(backupMatcher.group(1)));
            }
          }
        } catch (IOException e) {
          // Checkpoint files may be renamed or pruned concurrently while listing.
          return false;
        }
        for (Long ts : backupTimes) {
          if (ts > afterTimeMillis && checkpointTimes.contains(ts)) {
            return true;
          }
        }
      }
      return false;
    }, 2, TimeUnit.MINUTES);
  }
}
