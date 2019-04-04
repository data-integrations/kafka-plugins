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

package io.cdap.plugin.common;

import com.google.common.base.Strings;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Utility class for Kafka operations
 */
public final class KafkaHelpers {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaHelpers.class);
  public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

  // This class cannot be instantiated
  private KafkaHelpers() {
  }

  /**
   * Fetch the latest offsets for the given topic-partitions
   *
   * @param consumer The Kafka consumer
   * @param topicAndPartitions topic-partitions to fetch the offsets for
   * @return Mapping of topic-partiton to its latest offset
   */
  public static <K, V> Map<TopicPartition, Long> getLatestOffsets(Consumer<K, V> consumer,
                                                                  List<TopicPartition> topicAndPartitions) {
    consumer.assign(topicAndPartitions);
    consumer.seekToEnd(topicAndPartitions);

    Map<TopicPartition, Long> offsets = new HashMap<>();
    for (TopicPartition topicAndPartition : topicAndPartitions) {
      long offset = consumer.position(topicAndPartition);
      offsets.put(topicAndPartition, offset);
    }
    return offsets;
  }

  /**
   * Fetch the earliest offsets for the given topic-partitions
   *
   * @param consumer The Kafka consumer
   * @param topicAndPartitions topic-partitions to fetch the offsets for
   * @return Mapping of topic-partiton to its earliest offset
   */
  public static <K, V> Map<TopicPartition, Long> getEarliestOffsets(Consumer<K, V> consumer,
                                                                    List<TopicPartition> topicAndPartitions) {
    consumer.assign(topicAndPartitions);
    consumer.seekToBeginning(topicAndPartitions);

    Map<TopicPartition, Long> offsets = new HashMap<>();
    for (TopicPartition topicAndPartition : topicAndPartitions) {
      long offset = consumer.position(topicAndPartition);
      offsets.put(topicAndPartition, offset);
    }
    return offsets;
  }

  /**
   * Adds the JAAS conf to the Kafka configuration object for Kafka client login, if needed.
   * The JAAS conf is not added if either the principal or the keytab is null.
   *
   * @param conf Kafka configuration object to add the JAAS conf to
   * @param principal Kerberos principal
   * @param keytabLocation Kerberos keytab for the principal
   */
  public static void setupKerberosLogin(Map<String, ? super String> conf, @Nullable String principal,
                                        @Nullable String keytabLocation) {
    if (principal != null && keytabLocation != null) {
      LOG.debug("Adding Kerberos login conf to Kafka for principal {} and keytab {}",
                principal, keytabLocation);
      conf.put(SASL_JAAS_CONFIG, String.format("com.sun.security.auth.module.Krb5LoginModule required \n" +
                                                   "        useKeyTab=true \n" +
                                                   "        storeKey=true  \n" +
                                                   "        useTicketCache=false  \n" +
                                                   "        renewTicket=true  \n" +
                                                   "        keyTab=\"%s\" \n" +
                                                   "        principal=\"%s\";",
                                                 keytabLocation, principal));
    } else {
      LOG.debug("Not adding Kerberos login conf to Kafka since either the principal {} or the keytab {} is null",
                principal, keytabLocation);
    }
  }

  /**
   * Validates whether the principal and keytab are both set or both of them are null/empty
   *
   * @param principal Kerberos principal
   * @param keytab Kerberos keytab for the principal
   */
  public static void validateKerberosSetting(@Nullable String principal, @Nullable String keytab) {
    if (Strings.isNullOrEmpty(principal) != Strings.isNullOrEmpty(keytab)) {
      String emptyField = Strings.isNullOrEmpty(principal) ? "principal" : "keytab";
      String message = emptyField + " is empty. When Kerberos security is enabled for Kafka, " +
              "then both the principal and the keytab have " +
              "to be specified. If Kerberos is not enabled, then both should be empty.";
      throw new IllegalArgumentException(message);
    }
  }
}
