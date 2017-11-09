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

package co.cask.hydrator.plugin.batchSource;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import kafka.common.TopicAndPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


/**
 * Input format for a Kafka pull job.
 */
public class KafkaInputFormat extends InputFormat<KafkaKey, KafkaMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class);
  private static final String KAFKA_REQUEST = "kafka.request";

  private static final Type LIST_TYPE = new TypeToken<List<KafkaRequest>>() { }.getType();

  @Override
  public RecordReader<KafkaKey, KafkaMessage> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new KafkaRecordReader();
  }


  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Gson gson = new Gson();
    List<KafkaRequest> finalRequests = gson.fromJson(context.getConfiguration().get(KAFKA_REQUEST), LIST_TYPE);
    List<InputSplit> kafkaSplits = new ArrayList<>();

    for (KafkaRequest r : finalRequests) {
      KafkaSplit split = new KafkaSplit(r);
      kafkaSplits.add(split);
    }

    return kafkaSplits;
  }

  static List<KafkaRequest> saveKafkaRequests(Configuration conf, String topic, Map<String, String> kafkaConf,
                                              final Set<Integer> partitions,
                                              Map<TopicAndPartition, Long> initOffsets,
                                              KeyValueTable table) throws Exception {
    Properties properties = new Properties();
    properties.putAll(kafkaConf);
    try (Consumer consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      // Get Metadata for all topics
      @SuppressWarnings("unchecked") List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
      if (!partitions.isEmpty()) {
        Collection<PartitionInfo> filteredPartitionInfos =
          Collections2.filter(partitionInfos,
                              new Predicate<PartitionInfo>() {
                                @Override
                                public boolean apply(PartitionInfo input) {
                                  return partitions.contains(input.partition());
                                }
                              });
        partitionInfos = ImmutableList.copyOf(filteredPartitionInfos);
      }

      // Get the latest offsets and generate the KafkaRequests
      List<KafkaRequest> finalRequests = createKafkaRequests(consumer, kafkaConf, partitionInfos, initOffsets, table);

      Collections.sort(finalRequests, new Comparator<KafkaRequest>() {
        @Override
        public int compare(KafkaRequest r1, KafkaRequest r2) {
          return r1.getTopic().compareTo(r2.getTopic());
        }
      });

      // TODO: Understand this logic
      Map<KafkaRequest, KafkaKey> offsetKeys = new HashMap<>();
      for (KafkaRequest request : finalRequests) {
        KafkaKey key = offsetKeys.get(request);

        if (key != null) {
          request.setOffset(key.getOffset());
          request.setAvgMsgSize(key.getMessageSize());
        }

        if (request.getEarliestOffset() > request.getOffset() || request.getOffset() > request.getLastOffset()) {

          boolean offsetUnset = request.getOffset() == KafkaRequest.DEFAULT_OFFSET;
          // When the offset is unset, it means it's a new topic/partition, we also need to consume the earliest offset
          if (offsetUnset) {
            request.setOffset(request.getEarliestOffset());
            offsetKeys.put(
              request,
              new KafkaKey(request.getTopic(), request.getPartition(),
                           0, request.getOffset()));
          }
        }
      }
      conf.set(KAFKA_REQUEST, new Gson().toJson(finalRequests));
      return finalRequests;
    }
  }

  private static List<KafkaRequest> createKafkaRequests(Consumer consumer, Map<String, String> kafkaConf,
                                                        List<PartitionInfo> partitionInfos,
                                                        Map<TopicAndPartition, Long> offsets,
                                                        KeyValueTable table) {
    Collection<TopicPartition> topicPartitions =
      Collections2.transform(partitionInfos,
                             new Function<PartitionInfo, TopicPartition>() {
                            @Override
                            public TopicPartition apply(PartitionInfo input) {
                              return new TopicPartition(input.topic(), input.partition());
                            }
                          });
    Map<TopicPartition, Long> latestOffsets = getLatestOffsets(consumer, topicPartitions);
    Map<TopicPartition, Long> earliestOffsets = getEarliestOffsets(consumer, topicPartitions);

    List<KafkaRequest> requests = new ArrayList<>();
    for (PartitionInfo partitionInfo : partitionInfos) {
      TopicAndPartition topicAndPartition = new TopicAndPartition(partitionInfo.topic(), partitionInfo.partition());
      TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
      long latestOffset = latestOffsets.get(topicPartition);
      Long start;
      byte[] tableStart = table.read(topicAndPartition.toString());
      if (tableStart != null) {
        start = Bytes.toLong(tableStart);
      } else {
        start = offsets.containsKey(topicAndPartition) ? offsets.get(topicAndPartition) - 1 : null;
      }

      long earliestOffset = start == null || start == -2 ? earliestOffsets.get(topicPartition) : start;
      if (earliestOffset == -1) {
        earliestOffset = latestOffset;
      }
      LOG.debug("Getting kafka messages from topic {}, partition {}, with earlistOffset {}, latest offset {}",
                topicAndPartition.topic(), topicAndPartition.partition(), earliestOffset, latestOffset);
      KafkaRequest KafkaRequest = new KafkaRequest(kafkaConf, topicAndPartition.topic(), topicAndPartition.partition());
      KafkaRequest.setLatestOffset(latestOffset);
      KafkaRequest.setEarliestOffset(earliestOffset);
      requests.add(KafkaRequest);
    }
    return requests;
  }

  private static Map<TopicPartition, Long> getLatestOffsets(Consumer consumer,
                                                            Collection<TopicPartition> topicAndPartitions) {
    consumer.assign(topicAndPartitions);
    consumer.seekToEnd(topicAndPartitions);

    Map<TopicPartition, Long> offsets = new HashMap<>();
    for (TopicPartition topicAndPartition : topicAndPartitions) {
      long offset = consumer.position(topicAndPartition);
      offsets.put(topicAndPartition, offset);
    }
    return offsets;
  }

  private static Map<TopicPartition, Long> getEarliestOffsets(Consumer consumer,
                                                              Collection<TopicPartition> topicAndPartitions) {
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
