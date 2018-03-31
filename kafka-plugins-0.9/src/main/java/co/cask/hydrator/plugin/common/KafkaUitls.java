package co.cask.hydrator.plugin.common;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for Kafka operations
 */
public final class KafkaUitls {
  // This class cannot be instantiated
  private KafkaUitls() {
  }

  /**
   * Fetch the latest offsets for the given topic-partitions
   *
   * @param consumer The Kafka consumer
   * @param topicAndPartitions topic-partitions to fetch the offsets for
   * @return Mapping of topic-partiton to its latest offset
   */
  public static Map<TopicPartition, Long> getLatestOffsets(Consumer consumer,
                                                           List<TopicPartition> topicAndPartitions) {
    consumer.assign(topicAndPartitions);
    for (TopicPartition topicPartition : topicAndPartitions) {
      consumer.seekToEnd(topicPartition);
    }

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
  public static Map<TopicPartition, Long> getEarliestOffsets(Consumer consumer,
                                                             List<TopicPartition> topicAndPartitions) {
    consumer.assign(topicAndPartitions);
    for (TopicPartition topicPartition : topicAndPartitions) {
      consumer.seekToBeginning(topicPartition);
    }

    Map<TopicPartition, Long> offsets = new HashMap<>();
    for (TopicPartition topicAndPartition : topicAndPartitions) {
      long offset = consumer.position(topicAndPartition);
      offsets.put(topicAndPartition, offset);
    }
    return offsets;
  }
}
