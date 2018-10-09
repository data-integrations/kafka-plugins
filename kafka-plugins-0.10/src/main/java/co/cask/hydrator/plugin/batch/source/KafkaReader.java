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

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * A class which reads from the fetch results from kafka.
 */
public class KafkaReader {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaReader.class);
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  // index of context
  private final KafkaRequest kafkaRequest;
  private final Consumer<byte[], byte[]> consumer;

  private long currentOffset;
  private long lastOffset;
  private Iterator<ConsumerRecord<byte[], byte[]>> messageIter;


  /**
   * Construct using the json representation of the kafka request
   */
  public KafkaReader(KafkaRequest request) {
    kafkaRequest = request;
    currentOffset = request.getOffset();
    lastOffset = request.getLastOffset();

    // read data from queue
    Properties properties = new Properties();
    properties.putAll(request.getConf());
    consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
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
      ConsumerRecord<byte[], byte[]> consumerRecord = messageIter.next();

      byte[] keyBytes = consumerRecord.key();
      byte[] value = consumerRecord.value();
      if (value == null) {
        LOG.warn("Received message with null message.payload with topic {} and partition {}",
                 kafkaKey.getTopic(), kafkaKey.getPartition());
      }

      ByteBuffer payload = value == null ? ByteBuffer.wrap(EMPTY_BYTE_ARRAY) : ByteBuffer.wrap(value);
      ByteBuffer key = keyBytes == null ? ByteBuffer.wrap(EMPTY_BYTE_ARRAY) :  ByteBuffer.wrap(keyBytes);

      kafkaKey.clear();
      kafkaKey.set(kafkaRequest.getTopic(), kafkaRequest.getPartition(), currentOffset,
                   consumerRecord.offset() + 1);
      kafkaKey.setMessageSize(value == null ? -1 : value.length);
      currentOffset = consumerRecord.offset() + 1; // increase offset
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

    TopicPartition topicPartition = new TopicPartition(kafkaRequest.getTopic(), kafkaRequest.getPartition());
    this.consumer.assign(Lists.newArrayList(topicPartition));
    this.consumer.seek(topicPartition, currentOffset);
    ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(TimeUnit.SECONDS.toMillis(30));
    messageIter = consumerRecords.iterator();
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
    if (consumer != null) {
      consumer.close();
    }
  }
}
