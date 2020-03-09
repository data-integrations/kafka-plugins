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
import io.cdap.plugin.common.LineageRecorder;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Kafka Streaming source.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Kafka")
@Description("Kafka streaming source.")
public class KafkaStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
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

    return KafkaStreamingSourceUtil.getStructuredRecordJavaDStream(context, conf, collector);
  }
}
