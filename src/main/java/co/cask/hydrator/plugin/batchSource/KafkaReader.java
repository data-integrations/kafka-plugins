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

import kafka.api.PartitionFetchInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * A class which reads from the fetch results from kafka.
 */
public class KafkaReader {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaReader.class);

  // index of context
  private static final int fetchBufferSize = 1024 * 1024;
  private final KafkaRequest kafkaRequest;
  private final SimpleConsumer simpleConsumer;

  private long currentOffset;
  private long lastOffset;
  private Iterator<MessageAndOffset> messageIter;


  /**
   * Construct using the json representation of the kafka request
   */
  public KafkaReader(KafkaRequest request) {
    kafkaRequest = request;
    currentOffset = request.getOffset();
    lastOffset = request.getLastOffset();

    // read data from queue
    URI uri = kafkaRequest.getURI();
    simpleConsumer = new SimpleConsumer(uri.getHost(), uri.getPort(), 20 * 1000, fetchBufferSize, "client");
    fetch();
  }

  public boolean hasNext() throws IOException {
    if (currentOffset >= lastOffset) {
      return false;
    }
    if (messageIter != null && messageIter.hasNext()) {
      return true;
    } else {
      return fetch();
    }
  }

  /**
   * Fetches the next Kafka message and stuffs the results into the key and value.
   */
  public KafkaMessage getNext(KafkaKey kafkaKey) throws IOException {
    if (hasNext()) {

      MessageAndOffset msgAndOffset = messageIter.next();
      Message message = msgAndOffset.message();

      ByteBuffer payload = message.payload();
      ByteBuffer key = message.key();

      if (payload == null) {
        LOG.warn("Received message with null message.payload with topic {} and partition {}",
                 kafkaKey.getTopic(), kafkaKey.getPartition());

      }

      kafkaKey.clear();
      kafkaKey.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(), kafkaRequest.getPartition(), currentOffset,
                 msgAndOffset.offset() + 1, message.checksum());
      kafkaKey.setMessageSize(msgAndOffset.message().size());
      currentOffset = msgAndOffset.offset() + 1; // increase offset
      return new KafkaMessage(payload, key);
    } else {
      return null;
    }
  }

  /**
   * Creates a fetch request.
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

    FetchResponse fetchResponse;
    try {
      fetchResponse = simpleConsumer.fetch(fetchRequest);
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
   * Closes this context
   */
  public void close() throws IOException {
    if (simpleConsumer != null) {
      simpleConsumer.close();
    }
  }
}
