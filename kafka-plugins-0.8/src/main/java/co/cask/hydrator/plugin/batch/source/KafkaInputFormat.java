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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  public static List<KafkaRequest> saveKafkaRequests(Configuration conf, String topic, Map<String, Integer> brokers,
                                                     Set<Integer> partitions,
                                                     Map<TopicAndPartition, Long> initOffsets,
                                                     KeyValueTable table) throws Exception {
    ArrayList<KafkaRequest> finalRequests;
    HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo = new HashMap<>();

    // Get Metadata for all topics
    List<TopicMetadata> topicMetadataList = getKafkaMetadata(brokers, topic);

    for (TopicMetadata topicMetadata : topicMetadataList) {
      for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
        LeaderInfo leader =
          new LeaderInfo(new URI("tcp://" + partitionMetadata.leader().connectionString()),
                         partitionMetadata.leader().id());
        if (partitions.isEmpty() || partitions.contains(partitionMetadata.partitionId())) {
          if (offsetRequestInfo.containsKey(leader)) {
            ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
            topicAndPartitions.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId()));
            offsetRequestInfo.put(leader, topicAndPartitions);
          } else {
            ArrayList<TopicAndPartition> topicAndPartitions = new ArrayList<>();
            topicAndPartitions.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId()));
            offsetRequestInfo.put(leader, topicAndPartitions);
          }
        }
      }
    }

    // Get the latest offsets and generate the KafkaRequests
    finalRequests = fetchLatestOffsetAndCreateKafkaRequests(offsetRequestInfo, initOffsets, table);

    Collections.sort(finalRequests, new Comparator<KafkaRequest>() {
      @Override
      public int compare(KafkaRequest r1, KafkaRequest r2) {
        return r1.getTopic().compareTo(r2.getTopic());
      }
    });

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
            new KafkaKey(request.getTopic(), request.getLeaderId(), request.getPartition(), 0, request.getOffset()));
        }
      }
    }
    conf.set(KAFKA_REQUEST, new Gson().toJson(finalRequests));
    return finalRequests;
  }

  private static List<TopicMetadata> getKafkaMetadata(Map<String, Integer> brokers, String topic) {
    List<TopicMetadata> topicMetadataList = new ArrayList<>();

    for (Map.Entry<String, Integer> entry : brokers.entrySet()) {
      SimpleConsumer consumer = createSimpleConsumer(entry.getKey(), entry.getValue());
      LOG.debug("Fetching metadata from broker {}: {} with client id {} for topic {}", entry.getKey(),
                entry.getValue(), consumer.clientId(), topic);
      try {
        topicMetadataList =
          consumer.send(new TopicMetadataRequest(ImmutableList.of(topic))).topicsMetadata();
        break;
      } catch (Exception e) {
        // No-op just query next broker
      } finally  {
        consumer.close();
      }
    }

    if (topicMetadataList.isEmpty()) {
      throw new IllegalArgumentException(
        String.format("Failed to get any information for topic: %s from the given brokers: %s", topic,
                      brokers.toString()));
    }

    return topicMetadataList;
  }

  private static SimpleConsumer createSimpleConsumer(String host, int port) {
    return new SimpleConsumer(host, port, 20 * 1000, 128 * 1024, "client");
  }

  /**
   * Gets the latest offsets and create the requests as needed
   */
  private static ArrayList<KafkaRequest> fetchLatestOffsetAndCreateKafkaRequests(
    Map<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo,
    Map<TopicAndPartition, Long> offsets,
    KeyValueTable table) {
    ArrayList<KafkaRequest> finalRequests = new ArrayList<>();
    for (LeaderInfo leader : offsetRequestInfo.keySet()) {
      Long latestTime = kafka.api.OffsetRequest.LatestTime();
      Long earliestTime = kafka.api.OffsetRequest.EarliestTime();

      SimpleConsumer consumer = createSimpleConsumer(leader.getUri().getHost(), leader.getUri().getPort());
      // Latest Offset
      PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(latestTime, 1);
      // Earliest Offset
      PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo = new PartitionOffsetRequestInfo(earliestTime, 1);
      Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo = new HashMap<>();
      Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestOffsetInfo = new HashMap<>();
      ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
      for (TopicAndPartition topicAndPartition : topicAndPartitions) {
        latestOffsetInfo.put(topicAndPartition, partitionLatestOffsetRequestInfo);
        earliestOffsetInfo.put(topicAndPartition, partitionEarliestOffsetRequestInfo);
      }

      OffsetResponse latestOffsetResponse = getLatestOffsetResponse(consumer, latestOffsetInfo);
      OffsetResponse earliestOffsetResponse = null;
      if (latestOffsetResponse != null) {
        earliestOffsetResponse = getLatestOffsetResponse(consumer, earliestOffsetInfo);
      }
      consumer.close();
      if (earliestOffsetResponse == null) {
        continue;
      }

      for (TopicAndPartition topicAndPartition : topicAndPartitions) {
        long latestOffset = latestOffsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0];
        Long start;
        byte[] tableStart = table.read(topicAndPartition.toString());
        if (tableStart != null) {
          start = Bytes.toLong(tableStart);
        } else {
          start = offsets.containsKey(topicAndPartition) ? offsets.get(topicAndPartition) - 1 : null;
        }

        long earliestOffset = start == null || start == -2
          ? earliestOffsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0] : start;
        if (earliestOffset == -1) {
          earliestOffset = latestOffset;
        }
        LOG.debug("Getting kafka messages from topic {}, partition {}, with earlistOffset {}, latest offset {}",
                  topicAndPartition.topic(), topicAndPartition.partition(), earliestOffset, latestOffset);
        KafkaRequest kafkaRequest =
          new KafkaRequest(topicAndPartition.topic(), Integer.toString(leader.getLeaderId()),
                           topicAndPartition.partition(), leader.getUri());
        kafkaRequest.setLatestOffset(latestOffset);
        kafkaRequest.setEarliestOffset(earliestOffset);
        finalRequests.add(kafkaRequest);
      }
    }
    return finalRequests;
  }

  private static OffsetResponse getLatestOffsetResponse(SimpleConsumer consumer,
                                                 Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo) {

    OffsetResponse offsetResponse =
      consumer.getOffsetsBefore(new kafka.javaapi.OffsetRequest(offsetInfo, kafka.api.OffsetRequest.CurrentVersion(),
                                                                "client"));
    if (offsetResponse.hasError()) {
      for (TopicAndPartition key : offsetInfo.keySet()) {
        short errorCode = offsetResponse.errorCode(key.topic(), key.partition());
        if (errorCode != ErrorMapping.NoError()) {
          throw new RuntimeException(
            String.format("Error happens when getting the offset for topic %s and partition %d with error code %d",
                          key.topic(), key.partition(), errorCode));
        }
      }
    }
    return offsetResponse;
  }
}
