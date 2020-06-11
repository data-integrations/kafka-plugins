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

package io.cdap.plugin.confluent.common;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for Kafka operations
 */
public final class KafkaHelpers {

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
}
