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

package io.cdap.plugin.batch.source;

import kafka.api.PartitionFetchInfo;
import kafka.cluster.Broker;
import kafka.common.BrokerNotAvailableException;
import kafka.common.LeaderNotAvailableException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A class which reads from the fetch results from kafka.
 */
final class Kafka08Reader implements KafkaReader {
  private static final Logger LOG = LoggerFactory.getLogger(Kafka08Reader.class);

  // index of context
  private static final int fetchBufferSize = 1024 * 1024;
  private final KafkaRequest kafkaRequest;
  private final SimpleConsumer simpleConsumer;

  private long currentOffset;
  private long lastOffset;
  private Iterator<MessageAndOffset> messageIter;

  /**
   * Construct a reader based on the given {@link KafkaRequest}.
   */
  Kafka08Reader(KafkaRequest request) {
    this.kafkaRequest = request;
    this.currentOffset = request.getStartOffset();
    this.lastOffset = request.getEndOffset();

    // read data from queue
    Map<String, String> conf = request.getConf();
    Broker leader = getLeader(KafkaBatchConfig.parseBrokerMap(conf.get(KafkaInputFormat.KAFKA_BROKERS)),
                              request.getTopic(), request.getPartition());
    this.simpleConsumer = new SimpleConsumer(leader.host(), leader.port(), 20 * 1000, fetchBufferSize, "client");
  }

  @Override
  public boolean hasNext() {
    if (currentOffset >= lastOffset) {
      return false;
    }
    if (messageIter != null && messageIter.hasNext()) {
      return true;
    }
    return fetch();
  }

  /**
   * Fetches the next Kafka message and stuffs the results into the key and value.
   */
  @Override
  public KafkaMessage getNext(KafkaKey kafkaKey) {
    if (!hasNext()) {
      throw new NoSuchElementException("No message is available");
    }

    MessageAndOffset msgAndOffset = messageIter.next();
    Message message = msgAndOffset.message();

    ByteBuffer payload = message.payload();
    ByteBuffer key = message.key();

    if (payload == null) {
      LOG.warn("Received message with null message.payload with topic {} and partition {}",
               kafkaKey.getTopic(), kafkaKey.getPartition());

    }

    kafkaKey.set(currentOffset, msgAndOffset.offset() + 1, msgAndOffset.message().size(), message.checksum());
    currentOffset = msgAndOffset.offset() + 1; // increase offset
    return new KafkaMessage(payload, key);
  }

  /**
   * Fetch messages from Kafka.
   *
   * @return {@code true} if there is some messages available, {@code false} otherwise
   */
  private boolean fetch() {
    if (currentOffset >= lastOffset) {
      return false;
    }
    TopicAndPartition topicAndPartition = new TopicAndPartition(kafkaRequest.getTopic(), kafkaRequest.getPartition());
    PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(currentOffset, fetchBufferSize);

    Map<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<>();
    fetchInfo.put(topicAndPartition, partitionFetchInfo);

    FetchRequest fetchRequest = new FetchRequest(-1, "client", 1000, 1024, fetchInfo);

    try {
      FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
      if (fetchResponse.hasError()) {
        String message =
          "Error Code generated : " + fetchResponse.errorCode(kafkaRequest.getTopic(), kafkaRequest.getPartition());
        throw new RuntimeException(message);
      }
      return processFetchResponse(fetchResponse);
    } catch (Exception e) {
      return false;
    }
  }

  private boolean processFetchResponse(FetchResponse fetchResponse) {
    ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(kafkaRequest.getTopic(), kafkaRequest.getPartition());
    messageIter = messageBuffer.iterator();
    if (!messageIter.hasNext()) {
      messageIter = null;
      return false;
    }
    return true;
  }

  /**
   * Closes this reader
   */
  @Override
  public void close() {
    if (simpleConsumer != null) {
      simpleConsumer.close();
    }
  }

  /**
   * Gets the leader broker for the given topic partition.
   *
   * @param brokers the set of brokers to query from
   * @param topic the topic to query for
   * @param partition the partition to query for
   * @return the leader broker
   * @throws LeaderNotAvailableException if cannot find the leader broker
   */
  private static Broker getLeader(Map<String, Integer> brokers, String topic, int partition) {
    TopicMetadataRequest request = new TopicMetadataRequest(Collections.singletonList(topic));

    for (Map.Entry<String, Integer> entry : brokers.entrySet()) {
      SimpleConsumer consumer = new SimpleConsumer(entry.getKey(), entry.getValue(),
                                                   20 * 1000, fetchBufferSize, "client");
      try {
        for (TopicMetadata metadata : consumer.send(request).topicsMetadata()) {
          // This shouldn't happen. In case it does, just skip the metadata not for the right topic
          if (!topic.equals(metadata.topic())) {
            continue;
          }

          // Find the leader broker for the given partition.
          return metadata.partitionsMetadata().stream()
            .filter(meta -> meta.partitionId() == partition)
            .findFirst()
            .map(PartitionMetadata::leader)
            .orElseThrow(BrokerNotAvailableException::new);
        }
      } catch (Exception e) {
        // No-op just query next broker
      } finally  {
        consumer.close();
      }
    }

    throw new LeaderNotAvailableException(String.format("Failed to get broker information for partition %d in topic %s",
                                                        partition, topic));
  }
}
