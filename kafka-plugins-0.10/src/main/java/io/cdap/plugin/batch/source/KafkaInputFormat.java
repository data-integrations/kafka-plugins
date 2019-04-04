/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.plugin.batch.source;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.plugin.common.KafkaHelpers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Input format for a Kafka pull job.
 */
public class KafkaInputFormat extends InputFormat<KafkaKey, KafkaMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class);
  private static final String KAFKA_REQUEST = "kafka.request";

  private static final Type LIST_TYPE = new TypeToken<List<KafkaRequest>>() { }.getType();

  @Override
  public RecordReader<KafkaKey, KafkaMessage> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new KafkaRecordReader(Kafka10Reader::new);
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
   * @param kafkaConf extra Kafka consumer configurations
   * @param partitions the set of partitions to consume from.
   *                   If it is empty, it means reading from all available partitions under the given topic.
   * @param maxNumberRecords maximum number of records to read in one batch per partition
   * @param partitionOffsets the {@link KafkaPartitionOffsets} containing the starting offset for each partition
   * @return a {@link List} of {@link KafkaRequest} that get serialized in the hadoop configuration
   * @throws IOException if failed to setup the {@link KafkaRequest}
   */
  static List<KafkaRequest> saveKafkaRequests(Configuration conf, String topic, Map<String, String> kafkaConf,
                                              Set<Integer> partitions, long maxNumberRecords,
                                              KafkaPartitionOffsets partitionOffsets) throws IOException {
    Properties properties = new Properties();
    properties.putAll(kafkaConf);
    // change the request timeout to fetch the metadata to be 15 seconds or 1 second greater than session time out ms,
    // since this config has to be greater than the session time out, which is by default 10 seconds
    // the KafkaConsumer at runtime should still use the default timeout 305 seconds or whataver the user provides in
    // kafkaConf
    int requestTimeout = 15 * 1000;
    if (kafkaConf.containsKey(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG)) {
      requestTimeout = Math.max(requestTimeout,
                                Integer.valueOf(kafkaConf.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) + 1000));
    }
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
    try (Consumer<byte[], byte[]> consumer =
           new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      // Get all partitions for the given topic
      List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
      if (!partitions.isEmpty()) {
        // Filter it by the set of desired partitions
        partitionInfos = partitionInfos.stream()
          .filter(p -> partitions.contains(p.partition()))
          .collect(Collectors.toList());
      }

      // Get the latest offsets and generate the KafkaRequests
      List<KafkaRequest> finalRequests = createKafkaRequests(consumer, kafkaConf, partitionInfos,
                                                             maxNumberRecords, partitionOffsets);

      conf.set(KAFKA_REQUEST, new Gson().toJson(finalRequests));
      return finalRequests;
    }
  }

  /**
   * Creates a list of {@link KafkaRequest} by setting up the start and end offsets for each request. It may
   * query Kafka using the given {@link Consumer} for the earliest and latest offsets in the given set of partitions.
   */
  private static List<KafkaRequest> createKafkaRequests(Consumer<byte[], byte[]> consumer,
                                                        Map<String, String> kafkaConf,
                                                        List<PartitionInfo> partitionInfos,
                                                        long maxNumberRecords,
                                                        KafkaPartitionOffsets partitionOffsets) throws IOException {
    List<TopicPartition> topicPartitions = partitionInfos.stream()
      .map(info -> new TopicPartition(info.topic(), info.partition()))
      .collect(Collectors.toList());

    Map<TopicPartition, Long> latestOffsets = KafkaHelpers.getLatestOffsets(consumer, topicPartitions);
    Map<TopicPartition, Long> earliestOffsets = KafkaHelpers.getEarliestOffsets(consumer, topicPartitions);

    List<KafkaRequest> requests = new ArrayList<>();
    for (PartitionInfo partitionInfo : partitionInfos) {
      String topic = partitionInfo.topic();
      int partition = partitionInfo.partition();

      TopicPartition topicPartition = new TopicPartition(topic, partition);

      long startOffset = partitionOffsets.getPartitionOffset(partitionInfo.partition(),
                                                             earliestOffsets.getOrDefault(topicPartition, -1L));
      long endOffset = latestOffsets.get(topicPartition);
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

      LOG.debug("Getting kafka messages from topic {}, partition {}, with earlistOffset {}, latest offset {}",
                topic, partition, startOffset, endOffset);

      requests.add(new KafkaRequest(topic, partition, kafkaConf, startOffset, endOffset));
    }
    return requests;
  }
}
