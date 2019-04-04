package io.cdap.plugin.batch.source;

import java.io.Closeable;

interface KafkaReader extends Closeable {

  boolean hasNext();

  /**
   * Fetches the next Kafka message and stuffs the results into the key and value.
   */
  KafkaMessage getNext(KafkaKey kafkaKey);
}
