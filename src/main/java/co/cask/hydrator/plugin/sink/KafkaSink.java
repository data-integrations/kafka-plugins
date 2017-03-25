package co.cask.hydrator.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.collect.Lists;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka sink to write to Kafka
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("KafkaSink")
@Description("KafkaSink to write events to kafka")
public class KafkaSink extends ReferenceBatchSink<StructuredRecord, Text, PartitionMessageWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);

  // Configuration for the plugin.
  private final Config producerConfig;

  // Static constants for configuring Kafka producer.
  private static final String BROKER_LIST = "bootstrap.servers";
  private static final String KEY_SERIALIZER = "key.serializer";
  private static final String VAL_SERIALIZER = "value.serializer";
  private static final String ACKS_REQUIRED = "request.required.acks";

  public KafkaSink(Config producerConfig) {
    super(producerConfig);
    this.producerConfig = producerConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    if (!producerConfig.async.equalsIgnoreCase("true") && !producerConfig.async.equalsIgnoreCase("false")) {
      throw new IllegalArgumentException("Async flag has to be either TRUE or FALSE.");
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.of(producerConfig.referenceName, new KafkaOutputFormatProvider(producerConfig)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Text, PartitionMessageWritable>> emitter)
    throws Exception {
    LOG.info("in transform");
    List<Schema.Field> fields = input.getSchema().getFields();
    String body = "";

    // Depending on the configuration create a body that needs to be
    // built and pushed to Kafka.

    if (producerConfig.format.equalsIgnoreCase("JSON")){
      body = StructuredRecordStringConverter.toJsonString(input);
    }else{
      // Extract all values from the structured record
      List<Object> objs = Lists.newArrayList();
      for (Schema.Field field : fields) {
        objs.add(input.get(field.getName()));
      }

      StringWriter writer = new StringWriter();
      CSVPrinter printer = null;

      try {
        CSVFormat csvFileFormat;
        switch (producerConfig.format.toLowerCase()) {
          case "csv":
            LOG.info("Format is CSV");
            csvFileFormat = CSVFormat.Predefined.Default.getFormat();
            printer = new CSVPrinter(writer, csvFileFormat);
            break;
        }

        if (printer != null) {
          printer.printRecord(objs);
          body = writer.toString();
        }

      } finally {
        if (printer != null) {
          printer.close();
        }
      }
    }

    // Message key.
    String key = "no_key";
    if (producerConfig.key != null) {
      key = input.get(producerConfig.key);
    }

    int numOfPartitions = Integer.parseInt(producerConfig.numOfPartitions);
    int partitionKey = key.hashCode() % numOfPartitions;

    LOG.info("Emitting records");
    // emit records
    emitter.emit(new KeyValue<>(new Text(key),
                                new PartitionMessageWritable(new Text(body), new IntWritable(partitionKey))));
    LOG.info("Emittedrecords");
  }


  @Override
  public void destroy() {
    super.destroy();
  }

  /**
   * Kafka Producer Configuration.
   */
  public static class Config extends ReferencePluginConfig {

    @Name("brokers")
    @Description("Specifies the connection string where Producer can find one or more brokers to " +
      "determine the leader for each topic")
    @Macro
    private String brokers;

    @Name("async")
    @Description("Specifies whether an acknowledgment is required from broker that message was received. " +
      "Default is FALSE")
    @Macro
    private String async;

    @Name("key")
    @Description("Specify the key field to be used in the message")
    @Macro
    private String key;

    @Name("numOfPartitions")
    @Description("Specify number of partitions for a topic")
    @Macro
    private String numOfPartitions;

    @Name("topic")
    @Description("Topic to which message needs to be published")
    @Macro
    private String topic;

    @Name("format")
    @Description("Format a structured record should be converted to")
    @Macro
    private String format;

    public Config(String brokers, String async, String key, String numOfPartitions, String topic, String format) {
      super(String.format("Kafka_%s", topic));
      this.brokers = brokers;
      this.async = async;
      this.key = key;
      this.numOfPartitions = numOfPartitions;
      this.topic = topic;
      this.format = format;
    }
  }

  private static class KafkaOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    KafkaOutputFormatProvider(Config kafktopicsaSinkConfig) {
      this.conf = new HashMap<>();
      conf.put("topic", kafktopicsaSinkConfig.topic);
      conf.put(BROKER_LIST, kafktopicsaSinkConfig.brokers);
      conf.put(KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
      conf.put(VAL_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
      conf.put("async", kafktopicsaSinkConfig.async);
      if (kafktopicsaSinkConfig.async.equalsIgnoreCase("true")) {
        conf.put(ACKS_REQUIRED, "1");
      }
    }

    @Override
    public String getOutputFormatClassName() {
      return KafkaOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
