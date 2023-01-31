package io.cdap.plugin.kafka.source;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for KafkaStreamingSourceUtil
 */
public class KafkaStreamingSourceUtilTest {

  @Test
  public void testValidateSavedPartitionsValid() {
    Map<TopicPartition, Long> savedPartitions = new HashMap<>();
    TopicPartition partition1 = new TopicPartition("test-topic", 0);
    TopicPartition partition2 = new TopicPartition("test-topic", 1);
    savedPartitions.put(partition1, 100L);
    savedPartitions.put(partition2, 102L);
    Map<TopicPartition, Long> earliestOffsets = new HashMap<>();
    earliestOffsets.put(partition1, 0L);
    earliestOffsets.put(partition2, 0L);
    Map<TopicPartition, Long> latestOffsets = new HashMap<>();
    latestOffsets.put(partition1, 202L);
    latestOffsets.put(partition2, 200L);
    KafkaStreamingSourceUtil.validateSavedPartitions(savedPartitions, earliestOffsets, latestOffsets);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateSavedPartitionsInvalid() {
    Map<TopicPartition, Long> savedPartitions = new HashMap<>();
    TopicPartition partition1 = new TopicPartition("test-topic", 0);
    TopicPartition partition2 = new TopicPartition("test-topic", 1);
    savedPartitions.put(partition1, 100L);
    savedPartitions.put(partition2, 102L);
    Map<TopicPartition, Long> earliestOffsets = new HashMap<>();
    earliestOffsets.put(partition1, 0L);
    earliestOffsets.put(partition2, 0L);
    Map<TopicPartition, Long> latestOffsets = new HashMap<>();
    latestOffsets.put(partition1, 10L);
    latestOffsets.put(partition2, 0L);
    KafkaStreamingSourceUtil.validateSavedPartitions(savedPartitions, earliestOffsets, latestOffsets);
  }
}
