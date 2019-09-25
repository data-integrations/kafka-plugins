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

package io.cdap.plugin.batch.source;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.common.io.ByteBuffers;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.common.KafkaHelpers;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Kafka batch source.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(KafkaBatchSource.NAME)
@Description("Kafka batch source.")
public class KafkaBatchSource extends BatchSource<KafkaKey, KafkaMessage, StructuredRecord> {
  public static final String NAME = "Kafka";

  private final Kafka10BatchConfig config;
  private List<KafkaRequest> kafkaRequests;
  private Schema schema;
  private RecordFormat<ByteBuffer, StructuredRecord> recordFormat;
  private String messageField;
  private FileContext fileContext;
  private Path offsetsFile;

  /**
   * Config properties for the plugin.
   */
  public static class Kafka10BatchConfig extends KafkaBatchConfig {

    @Description("Additional kafka consumer properties to set.")
    @Macro
    @Nullable
    private String kafkaProperties;

    @Description("The kerberos principal used for the source when kerberos security is enabled for kafka.")
    @Macro
    @Nullable
    private String principal;

    @Description("The keytab location for the kerberos principal when kerberos security is enabled for kafka.")
    @Macro
    @Nullable
    private String keytabLocation;

    public Kafka10BatchConfig() {
      super();
    }

    public Kafka10BatchConfig(String brokers, String partitions, String topic, String initialPartitionOffsets) {
      super(brokers, partitions, topic, initialPartitionOffsets);
    }

    @Nullable
    public String getPrincipal() {
      return principal;
    }

    @Nullable
    public String getKeytabLocation() {
      return keytabLocation;
    }

    public Map<String, String> getKafkaProperties() {
      KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
      Map<String, String> conf = new HashMap<>();
      if (!Strings.isNullOrEmpty(kafkaProperties)) {
        for (KeyValue<String, String> keyVal : kvParser.parse(kafkaProperties)) {
          conf.put(keyVal.getKey(), keyVal.getValue());
        }
      }
      return conf;
    }

    public void validate(FailureCollector collector) {
      super.validate(collector);
      KafkaHelpers.validateKerberosSetting(principal, keytabLocation, collector);
    }
  }

  public KafkaBatchSource(Kafka10BatchConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    config.validate(failureCollector);
    stageConfigurer.setOutputSchema(config.getSchema(failureCollector));
    failureCollector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    FailureCollector failureCollector = context.getFailureCollector();
    KafkaPartitionOffsets partitionOffsets = config.getInitialPartitionOffsets(failureCollector);
    Schema schema = config.getSchema(failureCollector);
    Set<Integer> partitions = config.getPartitions(failureCollector);
    failureCollector.getOrThrowException();

    // If the offset directory is provided, try to load the file
    if (!context.isPreviewEnabled() && config.getOffsetDir() != null) {
      Path offsetDir = new Path(URI.create(config.getOffsetDir()));
      fileContext = FileContext.getFileContext(offsetDir.toUri(), conf);
      try {
        fileContext.mkdir(offsetDir, new FsPermission("700"), true);
      } catch (FileAlreadyExistsException e) {
        // It's ok if the parent already exists
      }
      offsetsFile = KafkaBatchConfig.getOffsetFilePath(offsetDir, context.getNamespace(), context.getPipelineName());

      // Load the offset from the offset file
      partitionOffsets = KafkaPartitionOffsets.load(fileContext, offsetsFile);
    }

    Map<String, String> kafkaConf = new HashMap<>();
    kafkaConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokers());
    // We save offsets in datasets, no need for Kafka to save them
    kafkaConf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    KafkaHelpers.setupKerberosLogin(kafkaConf, config.getPrincipal(), config.getKeytabLocation());
    kafkaConf.putAll(config.getKafkaProperties());
    kafkaRequests = KafkaInputFormat.saveKafkaRequests(conf, config.getTopic(), kafkaConf,
                                                       partitions,
                                                       config.getMaxNumberRecords(),
                                                       partitionOffsets);
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);

    if (schema != null) {
      lineageRecorder.createExternalDataset(schema);
      if (schema.getFields() != null && !schema.getFields().isEmpty()) {
        lineageRecorder.recordRead("Read", "Read from Kafka topic.",
                                   schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
      }
    }
    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(KafkaInputFormat.class, conf)));
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    if (context.isPreviewEnabled()) {
      return;
    }
    if (succeeded && kafkaRequests != null && fileContext != null && offsetsFile != null) {
      KafkaPartitionOffsets partitionOffsets = new KafkaPartitionOffsets(
        kafkaRequests.stream().collect(Collectors.toMap(KafkaRequest::getPartition, KafkaRequest::getEndOffset)));

      try {
        KafkaPartitionOffsets.save(fileContext, offsetsFile, partitionOffsets);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);

    schema = config.getSchema(context.getFailureCollector());
    Schema messageSchema = config.getMessageSchema(context.getFailureCollector());
    if (schema == null || messageSchema == null) {
      return;
    }
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      if (!name.equals(config.getKeyField()) && !name.equals(config.getPartitionField()) &&
        !name.equals(config.getOffsetField())) {
        messageField = name;
        break;
      }
    }
    if (config.getFormat() != null) {
      FormatSpecification spec = new FormatSpecification(config.getFormat(), messageSchema, new HashMap<>());
      recordFormat = RecordFormats.createInitializedFormat(spec);
    }
  }

  @Override
  public void transform(KeyValue<KafkaKey, KafkaMessage> input, Emitter<StructuredRecord> emitter) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (config.getKeyField() != null) {
      builder.set(config.getKeyField(), input.getValue().getKey().array());
    }
    if (config.getPartitionField() != null) {
      builder.set(config.getPartitionField(), input.getKey().getPartition());
    }
    if (config.getOffsetField() != null) {
      builder.set(config.getOffsetField(), input.getKey().getOffset());
    }
    if (config.getFormat() == null) {
      builder.set(messageField, ByteBuffers.getByteArray(input.getValue().getPayload()));
    } else {
      StructuredRecord messageRecord = recordFormat.read(input.getValue().getPayload());
      for (Schema.Field field : messageRecord.getSchema().getFields()) {
        String fieldName = field.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
    }
    emitter.emit(builder.build());
  }
}
