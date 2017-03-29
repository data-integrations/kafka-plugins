package co.cask.hydrator.plugin.sink;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
  private boolean isAsync;

  public KafkaRecordWriter(KafkaProducer<String, String> producer, String topic, boolean isAsync) {
    this.producer = producer;
    this.topic = topic;
    this.isAsync = isAsync;
  }

  protected void sendMessage(final String key, final String body) {
    if (isAsync) {
        producer.send(new ProducerRecord<>(topic, 0, key, body), new Callback() {
          @Override
          public void onCompletion(RecordMetadata meta, Exception e) {
            if (meta != null) {
              //success
            }

            if (e != null) {
              //error
              LOG.error("Exception while sending data to kafka topic {}, key {}, message {}, e", topic, key, body, e);
            }
          }
        });

    } else {
      // Waits infinitely to push the message through.
      try {
        producer.send(new ProducerRecord<>(topic, 0, key, body)).get();
      } catch (Exception e) {
        LOG.error("Exception while sending data to kafka topic {}, key {}, message {}, e", topic, key, body, e);
      }
    }
  }

  @Override
  public void write(Text key, Text value) throws IOException, InterruptedException {
    sendMessage(key.toString(), value.toString());
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (producer != null) {
      producer.close();
    }
  }
}
