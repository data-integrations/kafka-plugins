/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.confluent.streaming.sink;

import com.google.common.base.Strings;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Util method for {@link ConfluentStreamingSink}.
 *
 * This class contains methods for {@link ConfluentStreamingSink} that require spark classes because during validation
 * spark classes are not available. Refer CDAP-15912 for more information.
 */
final class ConfluentStreamingSinkUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ConfluentStreamingSinkUtil.class);

  @Nonnull
  public static Map<String, Object> getProducerParams(ConfluentStreamingSinkConfig conf, String pipelineName) {
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, conf.getBrokers());
    // Spark saves the offsets in checkpoints, no need for Kafka to save them
    kafkaParams.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    kafkaParams.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, "500");
    kafkaParams.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    kafkaParams.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    kafkaParams.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required " +
      "username=" + conf.getClusterApiKey() + " password=" + conf.getClusterApiSecret() + ";");

    if (!Strings.isNullOrEmpty(conf.getSchemaRegistryUrl())) {
      kafkaParams.put("schema.registry.url", conf.getSchemaRegistryUrl());
      kafkaParams.put("basic.auth.credentials.source", "USER_INFO");
      kafkaParams.put("schema.registry.basic.auth.user.info",
                      conf.getSchemaRegistryApiKey() + ":" + conf.getSchemaRegistryApiSecret());
      kafkaParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getCanonicalName());
      kafkaParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getCanonicalName());
    } else {
      kafkaParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
      kafkaParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    }
    kafkaParams.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, conf.getCompressionType());
    if (conf.getAsync()) {
      kafkaParams.put(ProducerConfig.ACKS_CONFIG, "1");
    }
    kafkaParams.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "20000");
    kafkaParams.putAll(conf.getKafkaProperties());
    return kafkaParams;
  }

  private ConfluentStreamingSinkUtil() {
    // no-op
  }
}
