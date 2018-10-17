/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Input format for a Kafka pull job.
 */
public class KafkaInputFormat extends InputFormat<KafkaKey, KafkaMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class);
  private static final String KAFKA_REQUEST = "kafka.request";

  static final String KAFKA_BROKERS = "kafka.brokers";

  private static final Type LIST_TYPE = new TypeToken<List<KafkaRequest>>() { }.getType();

  @Override
  public RecordReader<KafkaKey, KafkaMessage> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new KafkaRecordReader(Kafka08Reader::new);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    Gson gson = new Gson();
    List<KafkaRequest> finalRequests = gson.fromJson(context.getConfiguration().get(KAFKA_REQUEST), LIST_TYPE);
    List<InputSplit> kafkaSplits = new ArrayList<>();

    for (KafkaRequest r : finalRequests) {
      KafkaSplit split = new KafkaSplit(r);
      kafkaSplits.add(split);
    }

    return kafkaSplits;
  }

  /**
   * Generates and serializes a list of requests for reading from Kafka to the given {@link Configuration}.
   *
   * @param conf the hadoop configuration to update
   * @param topic the Kafka topic
   * @param brokers a {@link Map} from broker host to port
   * @param partitions the set of partitions to consume from.
   *                   If it is empty, it means reading from all available partitions under the given topic.
   * @param maxNumberRecords maximum number of records to read in one batch per partition
   * @param partitionOffsets the {@link KafkaPartitionOffsets} containing the starting offset for each partition
   * @return a {@link List} of {@link KafkaRequest} that get serialized in the hadoop configuration
   * @throws IOException if failed to setup the {@link KafkaRequest}
   */
  static List<KafkaRequest> saveKafkaRequests(Configuration conf, String topic,
                                              Map<String, Integer> brokers, Set<Integer> partitions,
                                              long maxNumberRecords,
                                              KafkaPartitionOffsets partitionOffsets) throws Exception {
    // Find the leader for each requested partition
    Map<Broker, Set<Integer>> brokerPartitions = getBrokerPartitions(brokers, topic, partitions);

    // Create and save the KafkaRequest
    List<KafkaRequest> requests = createKafkaRequests(topic, brokerPartitions, maxNumberRecords, partitionOffsets);

    conf.set(KAFKA_REQUEST, new Gson().toJson(requests));
    return requests;
  }

  /**
   * Returns a {@link Map} from {@link Broker} to the set of partitions that the given broker is a leader of.
   */
  private static Map<Broker, Set<Integer>> getBrokerPartitions(Map<String, Integer> brokers,
                                                               String topic, Set<Integer> partitions) {
    TopicMetadataRequest request = new TopicMetadataRequest(Collections.singletonList(topic));
    Map<Broker, Set<Integer>> result = new HashMap<>();
    Set<Integer> partitionsRemained = new HashSet<>(partitions);

    // Goes through the list of broker to fetch topic metadata. It uses the first one that returns
    for (Map.Entry<String, Integer> entry : brokers.entrySet()) {
      SimpleConsumer consumer = createSimpleConsumer(entry.getKey(), entry.getValue());
      LOG.debug("Fetching metadata from broker {}: {} with client id {} for topic {}", entry.getKey(),
                entry.getValue(), consumer.clientId(), topic);
      try {
        boolean hasError = false;
        for (TopicMetadata metadata : consumer.send(request).topicsMetadata()) {
          // This shouldn't happen. In case it does, just skip the metadata not for the right topic
          if (!topic.equals(metadata.topic())) {
            continue;
          }

          // Associate partition to leader broker
          for (PartitionMetadata partitionMetadata : metadata.partitionsMetadata()) {
            int partitionId = partitionMetadata.partitionId();

            // Skip error
            if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
              hasError = true;
              continue;
            }
            // Add the partition if either the user wants all partitions or user explicitly request a set.
            // If the user wants all partitions, the partitions set is empty
            if (partitions.isEmpty() || partitionsRemained.remove(partitionId)) {
              result.computeIfAbsent(partitionMetadata.leader(), k -> new HashSet<>()).add(partitionId);
            }
          }
        }

        // If there is no error and all partitions are needed, then we are done
        // Alternatively, if only a subset of partitions are needed and all of them are fetch, then we are also done
        if ((!hasError && partitions.isEmpty()) || (!partitions.isEmpty() && partitionsRemained.isEmpty())) {
          return result;
        }
      } catch (Exception e) {
        // No-op just query next broker
      } finally  {
        consumer.close();
      }
    }

    throw new IllegalArgumentException(
      String.format("Failed to get broker information for partitions %s in topic %s from the given brokers: %s",
                    partitionsRemained, topic, brokers));
  }

  private static SimpleConsumer createSimpleConsumer(String host, int port) {
    return new SimpleConsumer(host, port, 20 * 1000, 128 * 1024, "client");
  }

  /**
   * Creates a list of {@link KafkaRequest} by setting up the start and end offsets for each request. It may
   * query Kafka using the given set of brokers for the earliest and latest offsets in the given set of partitions.
   */
  private static List<KafkaRequest> createKafkaRequests(String topic, Map<Broker, Set<Integer>> brokerPartitions,
                                                        long maxNumberRecords,
                                                        KafkaPartitionOffsets partitionOffsets) throws IOException {
    List<KafkaRequest> result = new ArrayList<>();
    String brokerString = brokerPartitions.keySet().stream()
      .map(b -> b.host() + ":" + b.port()).collect(Collectors.joining(","));

    for (Map.Entry<Broker, Set<Integer>> entry : brokerPartitions.entrySet()) {
      Broker broker = entry.getKey();
      SimpleConsumer consumer = createSimpleConsumer(broker.host(), broker.port());
      try {
        // Get the latest offsets for partitions in this broker
        Map<Integer, Long> latestOffsets = getOffsetsBefore(consumer, topic, entry.getValue(),
                                                            kafka.api.OffsetRequest.LatestTime());

        // If there is no known partition offset for a given partition, also need to query for the earliest offset
        long earliestTime = kafka.api.OffsetRequest.EarliestTime();
        Set<Integer> earliestTimePartitions = entry.getValue().stream()
          .filter(p -> partitionOffsets.getPartitionOffset(p, earliestTime) == earliestTime)
          .collect(Collectors.toSet());

        Map<Integer, Long> earliestOffsets = getOffsetsBefore(consumer, topic, earliestTimePartitions, earliestTime);

        // Add KafkaRequest objects for the partitions in this broker
        for (int partition : entry.getValue()) {
          long startOffset = partitionOffsets.getPartitionOffset(partition,
                                                                 earliestOffsets.getOrDefault(partition, -1L));
          long endOffset = latestOffsets.getOrDefault(partition, -1L);

          // StartOffset shouldn't be negative, as it should either in the partitionOffsets or in the earlierOffsets
          if (startOffset < 0) {
            throw new IOException("Failed to find start offset for topic " + topic + " and partition " + partition);
          }
          // Also, end offset shouldn't be negative.
          if (endOffset < 0) {
            throw new IOException("Failed to find end offset for topic " + topic + " and partition " + partition);
          }

          // Limit the number of records fetched
          if (maxNumberRecords > 0) {
            endOffset = Math.min(endOffset, startOffset + maxNumberRecords);
          }

          LOG.debug("Getting kafka messages from topic {}, partition {}, with start offset {}, end offset {}",
                    topic, partition, startOffset,  endOffset);

          result.add(new KafkaRequest(topic, partition, Collections.singletonMap(KAFKA_BROKERS, brokerString),
                                      startOffset, endOffset));
        }

      } finally {
        consumer.close();
      }
    }

    return result;
  }

  /**
   * Queries Kafka for the offsets before the given time for the given set of partitions.
   */
  private static Map<Integer, Long> getOffsetsBefore(SimpleConsumer consumer, String topic,
                                                     Set<Integer> partitions, long time) throws IOException {
    if (partitions.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<TopicAndPartition, PartitionOffsetRequestInfo> request =
      partitions.stream().collect(Collectors.toMap(p -> new TopicAndPartition(topic, p),
                                                   p -> new PartitionOffsetRequestInfo(time, 1)));
    OffsetResponse response = consumer.getOffsetsBefore(new OffsetRequest(request,
                                                                          kafka.api.OffsetRequest.CurrentVersion(),
                                                                          "client"));
    if (response.hasError()) {
      for (int partition : partitions) {
        short errorCode = response.errorCode(topic, partition);
        if (errorCode != ErrorMapping.NoError()) {
          throw new RuntimeException(
            String.format("Failed to get the offset for topic %s and partition %d with error code %d",
                          topic, partition, errorCode));
        }
      }

      // This shouldn't happen
      throw new IOException("Failed to get offsets for topic " + topic + " and partitions " + partitions);
    }

    return partitions.stream().collect(Collectors.toMap(p -> p, p -> response.offsets(topic, p)[0]));
  }
}
