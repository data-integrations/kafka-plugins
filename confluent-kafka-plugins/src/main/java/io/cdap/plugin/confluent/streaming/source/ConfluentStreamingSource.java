/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.confluent.streaming.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.Constants;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Confluent Kafka Streaming source.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(ConfluentStreamingSource.PLUGIN_NAME)
@Description("Confluent Kafka streaming source.")
public class ConfluentStreamingSource extends StreamingSource<StructuredRecord> {
  public static final String PLUGIN_NAME = "Confluent";

  private final ConfluentStreamingSourceConfig conf;

  public ConfluentStreamingSource(ConfluentStreamingSourceConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    conf.validate(collector);
    Schema schema = getOutputSchema(collector);
    stageConfigurer.setOutputSchema(schema);

    if (conf.getMaxRatePerPartition() != null && conf.getMaxRatePerPartition() > 0) {
      Map<String, String> pipelineProperties = new HashMap<>();
      pipelineProperties.put("spark.streaming.kafka.maxRatePerPartition", conf.getMaxRatePerPartition().toString());
      pipelineConfigurer.setPipelineProperties(pipelineProperties);
    }
    pipelineConfigurer.createDataset(conf.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    conf.validate(collector);
    Schema outputSchema = getOutputSchema(collector);
    collector.getOrThrowException();

    context.registerLineage(conf.referenceName);
    return ConfluentStreamingSourceUtil.getStructuredRecordJavaDStream(context, conf, outputSchema, collector);
  }

  private Schema getOutputSchema(FailureCollector failureCollector) {
    if (conf.getSchemaRegistryUrl() == null) {
      return conf.getSchema(failureCollector);
    }
    return inferSchema(failureCollector);
  }

  private Schema inferSchema(FailureCollector failureCollector) {
    try {
      Map<String, Object> options = new HashMap<>();
      options.put("basic.auth.credentials.source", "USER_INFO");
      options.put("basic.auth.user.info", conf.getSchemaRegistryApiKey() + ':' + conf.getSchemaRegistryApiSecret());
      CachedSchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(conf.getSchemaRegistryUrl(), 2, options);
      Schema initialSchema = conf.getSchema(failureCollector);
      List<Schema.Field> newFields = new ArrayList<>();
      boolean keySchemaShouldBeAdded = conf.getKeyField() != null;
      boolean messageSchemaShouldBeAdded = conf.getValueField() != null;
      for (Schema.Field field : initialSchema.getFields()) {
        if (field.getName().equals(conf.getKeyField())) {
          Schema keySchema = fetchSchema(schemaRegistryClient, conf.getTopic() + "-key");
          newFields.add(Schema.Field.of(field.getName(), keySchema));
          keySchemaShouldBeAdded = false;
        } else if (field.getName().equals(conf.getValueField())) {
          Schema valueSchema = fetchSchema(schemaRegistryClient, conf.getTopic() + "-value");
          newFields.add(Schema.Field.of(field.getName(), valueSchema));
          messageSchemaShouldBeAdded = false;
        } else {
          newFields.add(field);
        }
      }
      if (keySchemaShouldBeAdded) {
        Schema keySchema = fetchSchema(schemaRegistryClient, conf.getTopic() + "-key");
        newFields.add(Schema.Field.of(conf.getKeyField(), keySchema));
      }
      if (messageSchemaShouldBeAdded) {
        Schema valueSchema = fetchSchema(schemaRegistryClient, conf.getTopic() + "-value");
        newFields.add(Schema.Field.of(conf.getValueField(), valueSchema));
      }
      return Schema.recordOf(initialSchema.getRecordName(), newFields);
    } catch (IOException | RestClientException e) {
      failureCollector.addFailure("Failed to infer output schema. Reason: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
      throw failureCollector.getOrThrowException();
    }
  }

  private Schema fetchSchema(CachedSchemaRegistryClient schemaRegistryClient, String subject)
    throws IOException, RestClientException {
    SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
    if (schemaMetadata.getSchema().startsWith("\"")) {
      String typeName = schemaMetadata.getSchema().substring(1, schemaMetadata.getSchema().length() - 1);
      return Schema.of(Schema.Type.valueOf(typeName.toUpperCase()));
    }
    return Schema.parseJson(schemaMetadata.getSchema());
  }
}
