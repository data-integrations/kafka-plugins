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

package io.cdap.plugin.kafka.alertpublisher;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.AlertPublisherContext;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.kafka.common.KafkaHelpers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Kafka Alert Publisher
 */
@Plugin(type = AlertPublisher.PLUGIN_TYPE)
@Name("KafkaAlerts")
public class KafkaAlertPublisher extends AlertPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaAlertPublisher.class);
  private static final Gson GSON = new Gson();
  private final Config config;

  private KafkaProducer<String, String> producer;

  public KafkaAlertPublisher(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void initialize(AlertPublisherContext context) throws Exception {
    super.initialize(context);
    config.validate(context.getFailureCollector());
    context.getFailureCollector().getOrThrowException();
    Properties props = new Properties();
    // Add client id property with stage name as value.
    props.put(ProducerConfig.CLIENT_ID_CONFIG, context.getStageName());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put("producer.type", "sync");

    // Override any property set above with user specified producer properties
    for (Map.Entry<String, String> producerProperty : config.getProducerProperties().entrySet()) {
      props.put(producerProperty.getKey(), producerProperty.getValue());
    }

    this.producer = new KafkaProducer<>(props);
  }

  @Override
  public void publish(Iterator<Alert> iterator) throws Exception {
    while (iterator.hasNext()) {
      String alert = GSON.toJson(iterator.next());
      try {
        // We do not specify key here. So the topic partitions will be chosen in round robin fashion.
        ProducerRecord<String, String> record = new ProducerRecord<>(config.topic, alert);
        producer.send(record);
      } catch (Exception e) {
        // catch the exception and continue processing rest of the alerts
        LOG.error("Exception while emitting alert {}", alert, e);
      }

    }
  }

  @Override
  public void destroy() {
    super.destroy();
    producer.close();
  }

  /**
   * Kafka Producer Configuration.
   */
  public static class Config extends PluginConfig {

    public static final String TOPIC = "topic";
    @Name("brokers")
    @Description("Specifies the connection string where Producer can find one or more brokers to " +
      "determine the leader for each topic.")
    @Macro
    private String brokers;

    @Name("topic")
    @Description("Topic to which message needs to be published. The topic should already exist on kafka.")
    @Macro
    private String topic;

    @Name("producerProperties")
    @Nullable
    @Description("Additional kafka producer properties to set.")
    private String producerProperties;

    @Description("The kerberos principal used for the source when kerberos security is enabled for kafka.")
    @Macro
    @Nullable
    private String principal;

    @Description("The keytab location for the kerberos principal when kerberos security is enabled for kafka.")
    @Macro
    @Nullable
    private String keytabLocation;

    public Config(String brokers, String topic, String producerProperties) {
      this.brokers = brokers;
      this.topic = topic;
      this.producerProperties = producerProperties;
    }

    private Map<String, String> getProducerProperties() {
      KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
      Map<String, String> producerProps = new HashMap<>();
      if (!Strings.isNullOrEmpty(producerProperties)) {
        for (KeyValue<String, String> keyVal : kvParser.parse(producerProperties)) {
          String key = keyVal.getKey();
          String val = keyVal.getValue();
          producerProps.put(key, val);
        }
      }
      return producerProps;
    }

    private void validate(FailureCollector collector) {
      // If the topic or brokers are macros they would not be available at config time. So do not perform
      // validations yet.
      if (Strings.isNullOrEmpty(topic) || Strings.isNullOrEmpty(brokers)) {
        return;
      }

      try {
        Topic.validate(topic);
      } catch (InvalidTopicException e) {
        collector.addFailure(String.format(
          "Topic name %s is not a valid kafka topic. Please provide valid kafka topic name. %s", topic,
          e.getMessage()), null)
          .withConfigProperty(TOPIC);
      }

      KafkaHelpers.validateKerberosSetting(principal, keytabLocation, collector);
    }
  }
}
