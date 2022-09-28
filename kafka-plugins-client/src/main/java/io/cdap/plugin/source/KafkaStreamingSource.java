/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.plugin.source;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.cdap.etl.api.streaming.StreamingStateHandler;
import io.cdap.plugin.batch.source.KafkaPartitionOffsets;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Kafka Streaming source.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Kafka")
@Description("Kafka streaming source.")
public class KafkaStreamingSource extends ReferenceStreamingSource<StructuredRecord> implements StreamingStateHandler {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamingSource.class);
  private static final Gson gson = new Gson();

  private final KafkaConfig conf;

  public KafkaStreamingSource(KafkaConfig conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    conf.validate(collector);
    Schema schema = conf.getSchema(collector);
    stageConfigurer.setOutputSchema(schema);

    if (conf.getMaxRatePerPartition() != null && conf.getMaxRatePerPartition() > 0) {
      Map<String, String> pipelineProperties = new HashMap<>();
      pipelineProperties.put("spark.streaming.kafka.maxRatePerPartition", conf.getMaxRatePerPartition().toString());
      pipelineConfigurer.setPipelineProperties(pipelineProperties);
    }
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    Schema schema = conf.getSchema(context.getFailureCollector());
    // record dataset lineage
    context.registerLineage(conf.referenceName, schema);

    if (schema != null && schema.getFields() != null) {
      LineageRecorder recorder = new LineageRecorder(context, conf.referenceName);
      recorder.recordRead("Read", "Read from Kafka",
                          schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    conf.getMessageSchema(collector);
    collector.getOrThrowException();

    JavaInputDStream<ConsumerRecord<byte[], byte[]>> recordJavaDStream =
      KafkaStreamingSourceUtil.getConsumerRecordJavaDStream(context, conf, collector, getStateSupplier(context));
    if (!context.isStateStoreEnabled()) {
      // Return the serializable Dstream in case checkpointing is enabled.
      return KafkaStreamingSourceUtil.getStructuredRecordJavaDStream(conf, recordJavaDStream);
    }

    // Use the DStream that is state aware
    KafkaDStream kafkaDStream = new KafkaDStream(context.getSparkStreamingContext().ssc(),
                                                 recordJavaDStream.inputDStream(),
                                                 KafkaStreamingSourceUtil.getRecordTransformFunction(conf),
                                                 getStateConsumer(context));
    return kafkaDStream.convertToJavaDStream();
  }

  private VoidFunction<OffsetRange[]> getStateConsumer(StreamingContext context) {
    return offsetRanges -> {
      try {
        saveState(context, offsetRanges);
      } catch (IOException e) {
        LOG.warn("Exception in saving state.", e);
      }
    };
  }

  private void saveState(StreamingContext context, OffsetRange[] offsetRanges) throws IOException {
    if (offsetRanges.length > 0) {
      Map<Integer, Long> partitionOffsetMap = Arrays.stream(offsetRanges)
        .collect(Collectors.toMap(OffsetRange::partition, OffsetRange::untilOffset));
      byte[] state = gson.toJson(new KafkaPartitionOffsets(partitionOffsetMap)).getBytes(StandardCharsets.UTF_8);
      context.saveState(conf.getTopic(), state);
    }
  }

  private Supplier<Map<TopicPartition, Long>> getStateSupplier(StreamingContext context) {
    return () -> {
      try {
        return getSavedState(context);
      } catch (IOException e) {
        throw new RuntimeException("Exception in fetching state.", e);
      }
    };
  }

  private Map<TopicPartition, Long> getSavedState(StreamingContext context) throws IOException {
    //State store is not enabled, do not read state
    if (!context.isStateStoreEnabled()) {
      return Collections.emptyMap();
    }

    //If state is not present, use configured offsets or defaults
    Optional<byte[]> state = context.getState(conf.getTopic());
    if (!state.isPresent()) {
      return Collections.emptyMap();
    }

    byte[] bytes = state.get();
    try (Reader reader = new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
      KafkaPartitionOffsets partitionOffsets = gson.fromJson(reader, KafkaPartitionOffsets.class);
      return partitionOffsets.getPartitionOffsets().entrySet()
        .stream()
        .collect(Collectors.toMap(partitionOffset -> new TopicPartition(conf.getTopic(), partitionOffset.getKey()),
                                  Map.Entry::getValue));
    }
  }
}
