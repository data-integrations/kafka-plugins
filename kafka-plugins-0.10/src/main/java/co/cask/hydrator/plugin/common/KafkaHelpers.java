package co.cask.hydrator.plugin.common;

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
      conf.put("sasl.jaas.config", String.format("com.sun.security.auth.module.Krb5LoginModule required \n" +
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
}
