package co.cask.hydrator.plugin.batch.sink;

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
public class KafkaRecordWriter extends RecordWriter<Text, PartitionMessageWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordWriter.class);
  private KafkaProducer<String, String> producer;
  private String[] topics;
  private boolean isAsync;

  public KafkaRecordWriter(KafkaProducer<String, String> producer, String[] topics, boolean isAsync) {
    this.producer = producer;
    this.topics = topics;
    this.isAsync = isAsync;
  }

  protected void sendMessage(final String key, final int partitionKey, final String body) {
    for (final String topic : topics) {
      if (isAsync) {
        producer.send(new ProducerRecord<>(topic, partitionKey, key, body), new Callback() {
          @Override
          public void onCompletion(RecordMetadata meta, Exception e) {
            if (meta != null) {
              //success
            }

            if (e != null) {
              //error
              LOG.error("Exception while sending data to kafka topic {}, partition: {}, key {}, message {}, e",
                        topic, partitionKey, key, body, e);
            }
          }
        });
      } else {
        // Waits infinitely to push the message through.
        try {
          producer.send(new ProducerRecord<>(topic, partitionKey, key, body)).get();
        } catch (Exception e) {
          LOG.error("Exception while sending data to kafka topic {}, partition: {}, key {}, message {}, e", topic,
                    partitionKey, key, body, e);
        }
      }
    }
  }

  @Override
  public void write(Text key, PartitionMessageWritable value) throws IOException, InterruptedException {
    sendMessage(key.toString(), value.getParitionKey().get(), value.getValue().toString());
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (producer != null) {
      producer.close();
    }
  }
}
