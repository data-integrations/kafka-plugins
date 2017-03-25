package co.cask.hydrator.plugin.sink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Output format to write to kafka
 */
public class KafkaOutputFormat extends OutputFormat<Text, PartitionMessageWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOutputFormat.class);

  private KafkaProducer<String, String> producer;
  // Static constants for configuring Kafka producer.
  private static final String BROKER_LIST = "bootstrap.servers";
  private static final String KEY_SERIALIZER = "key.serializer";
  private static final String VAL_SERIALIZER = "value.serializer";
  private static final String ACKS_REQUIRED = "request.required.acks";

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException,
    InterruptedException {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) throws IOException {
        // no-op
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) throws IOException {
        //no-op
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) throws IOException {
        //no-op
      }

      @Override
      public void abortTask(TaskAttemptContext taskContext) throws IOException {
        //no-op
      }
    };
  }

  @Override
  public RecordWriter<Text, PartitionMessageWritable> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();
    // Extract the topics
    String topic = configuration.get("topic");

    Properties props = new Properties();

    // Configure the properties for kafka.
    props.put(BROKER_LIST, configuration.get(BROKER_LIST));
    props.put(KEY_SERIALIZER, configuration.get(KEY_SERIALIZER));
    props.put(VAL_SERIALIZER, configuration.get(VAL_SERIALIZER));
    boolean isAsync = false;
    if (configuration.get("async") != null && configuration.get("async").equalsIgnoreCase("true")) {
      props.put(ACKS_REQUIRED, "1");
      isAsync = true;
    }

    //config = new ProducerConfig(props);
    producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

    return new KafkaRecordWriter(producer, topic, isAsync);
  }
}

