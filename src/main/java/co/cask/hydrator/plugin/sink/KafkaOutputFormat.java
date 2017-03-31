package co.cask.hydrator.plugin.sink;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Output format to write to kafka
 */
public class KafkaOutputFormat extends OutputFormat<Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOutputFormat.class);

  // Static constants for configuring Kafka producer.
  private static final String BROKER_LIST = "bootstrap.servers";
  private static final String KEY_SERIALIZER = "key.serializer";
  private static final String VAL_SERIALIZER = "value.serializer";
  private KafkaProducer<String, String> producer;


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
  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();

    // Extract the topics
    String topic = configuration.get("topic");

    Properties props = new Properties();
    // Configure the properties for kafka.
    props.put(BROKER_LIST, configuration.get(BROKER_LIST));
    props.put(KEY_SERIALIZER, configuration.get(KEY_SERIALIZER));
    props.put(VAL_SERIALIZER, configuration.get(VAL_SERIALIZER));
    props.put("compression.type", configuration.get("compression.type"));

    if (!Strings.isNullOrEmpty(configuration.get("hasKey"))) {
      // set partitioner class only if key is provided
      props.put("partitioner.class", "co.cask.hydrator.plugin.sink.StringPartitioner");
    }

    boolean isAsync = false;
    if (configuration.get("async") != null && configuration.get("async").equalsIgnoreCase("true")) {
      props.put(ProducerConfig.ACKS_CONFIG, "1");
      isAsync = true;
    }

    Map<String, String> valByRegex = configuration.getValByRegex("^additional.");

    for (Map.Entry<String, String> entry : valByRegex.entrySet()) {
      //strip off the prefix we added while creating the conf.
      props.put(entry.getKey().substring(11), entry.getValue());
      LOG.info("Property key: {}, value: {}", entry.getKey().substring(11), entry.getValue());
    }

    // CDAP-9178: cached the producer object to avoid being created on every batch interval
    if (producer == null) {
      producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    return new KafkaRecordWriter(producer, topic, isAsync);
  }
}

