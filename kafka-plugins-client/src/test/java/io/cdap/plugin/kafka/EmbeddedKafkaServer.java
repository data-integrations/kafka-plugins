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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.zookeeper.ZooKeeperClientExpiredException;
import kafka.zookeeper.ZooKeeperClientTimeoutException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.apache.twill.internal.utils.Networks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.net.BindException;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A {@link com.google.common.util.concurrent.Service} implementation for running an instance of Kafka server in
 * the same process.
 */
public final class EmbeddedKafkaServer extends AbstractIdleService {

  public static final String START_RETRIES = "twill.kafka.start.timeout.retries";

  private static final Logger LOG = LoggerFactory.getLogger(org.apache.twill.internal.kafka.EmbeddedKafkaServer.class);
  private static final String DEFAULT_START_RETRIES = "5";

  private final int startTimeoutRetries;
  private final Properties properties;
  private KafkaServer server;

  public EmbeddedKafkaServer(Properties properties) {
    this.startTimeoutRetries = Integer.parseInt(properties.getProperty(START_RETRIES,
                                                                       DEFAULT_START_RETRIES));
    this.properties = new Properties();
    this.properties.putAll(properties);
  }

  @Override
  protected void startUp() throws Exception {
    int tries = 0;
    do {
      KafkaConfig kafkaConfig = createKafkaConfig(properties);
      KafkaServer kafkaServer = createKafkaServer(kafkaConfig);
      try {
        kafkaServer.startup();
        server = kafkaServer;
      } catch (Exception e) {
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();

        Throwable rootCause = Throwables.getRootCause(e);
        if (rootCause instanceof ZooKeeperClientTimeoutException) {
          // Potentially caused by race condition bug described in TWILL-139.
          LOG.warn("Timeout when connecting to ZooKeeper from KafkaServer. Attempt number {}.", tries, rootCause);
        } else if (rootCause instanceof ZooKeeperClientExpiredException) {
          // Potentially caused by race condition bug described in TWILL-139.
          LOG.warn("Session expired when connecting to ZooKeeper from KafkaServer. Attempt number {}.",
                   tries, rootCause);
        } else if (rootCause instanceof BindException) {
          LOG.warn("Kafka failed to bind to port {}. Attempt number {}.", kafkaConfig.port(), tries, rootCause);
        } else {
          LOG.error("Exception when connecting to Zookeeper", e);
          throw e;
        }

        // Do a random sleep of < 200ms
        TimeUnit.MILLISECONDS.sleep(new Random().nextInt(200) + 1L);
      }
    } while (server == null && ++tries < startTimeoutRetries);

    if (server == null) {
      throw new IllegalStateException("Failed to start Kafka server after " + tries + " attempts.");
    }
  }

  @Override
  protected void shutDown() throws Exception {
    if (server != null) {
      server.shutdown();
      server.awaitShutdown();
    }
  }

  private KafkaServer createKafkaServer(KafkaConfig kafkaConfig) {
    Seq<KafkaMetricsReporter> metricsReporters =
      JavaConverters.collectionAsScalaIterableConverter(
        Collections.<KafkaMetricsReporter>emptyList()).asScala().toSeq();
    return new KafkaServer(kafkaConfig, new Time() {

      @Override
      public long milliseconds() {
        return System.currentTimeMillis();
      }

      @Override
      public long nanoseconds() {
        return System.nanoTime();
      }

      @Override
      public void sleep(long ms) {
        try {
          Thread.sleep(ms);
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
      }

      @Override
      public void waitObject(Object o, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException {
        while (!condition.get()) {
          long currentTimeMs = this.milliseconds();
          if (currentTimeMs >= deadlineMs) {
            throw new TimeoutException("Condition not satisfied before deadline");
          }
          o.wait(deadlineMs - currentTimeMs);
        }
      }

      @Override
      public long hiResClockMs() {
        return System.currentTimeMillis();
      }
    }, Option.apply("embedded-server"), metricsReporters);
  }

  /**
   * Creates a new {@link KafkaConfig} from the given {@link Properties}. If the {@code "port"} property is missing
   * or is equals to {@code "0"}, a random port will be generated.
   */
  private KafkaConfig createKafkaConfig(Properties properties) {
    Properties prop = new Properties();
    prop.putAll(properties);

    String port = prop.getProperty("port");
    if (port == null || "0".equals(port)) {
      int randomPort = Networks.getRandomPort();
      Preconditions.checkState(randomPort > 0, "Failed to get random port.");
      prop.setProperty("port", Integer.toString(randomPort));
    }

    return new KafkaConfig(prop);
  }
}
