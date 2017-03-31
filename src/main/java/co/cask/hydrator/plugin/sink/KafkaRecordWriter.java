package co.cask.hydrator.plugin.sink;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Record writer to write events to kafka
 */
public class KafkaRecordWriter extends RecordWriter<Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordWriter.class);
  private KafkaProducer<String, String> producer;
  private String topic;

  public KafkaRecordWriter(KafkaProducer<String, String> producer, String topic) {
    this.producer = producer;
    this.topic = topic;
  }

  protected void sendMessage(final String key, final String body) {
    try {
      producer.send(new ProducerRecord<>(topic, key, body)).get();
    } catch (Exception e) {
      LOG.error("Exception while sending data to kafka topic {}, key {}, message {}, e", topic, key, body, e);
    }
  }

  @Override
  public void write(Text key, Text value) throws IOException, InterruptedException {
    if (key == null) {
      sendMessage(null, value.toString());
    } else {
      sendMessage(key.toString(), value.toString());
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (producer != null) {
      producer.close();
    }
  }
}
