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

package io.cdap.plugin.confluent.streaming.sink;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.format.avro.StructuredToAvroTransformer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Create ProducerRecords from StructuredRecords.
 */
public class StructuredToProducerRecordTransformer implements Serializable {

  private static final long serialVersionUID = 1025599828133261290L;

  private final ConfluentStreamingSinkConfig conf;
  private final ConverterFunction<StructuredRecord, Object> keyExtractor;
  private final ConverterFunction<StructuredRecord, Object> valueExtractor;
  private final ConverterFunction<StructuredRecord, Integer> partitionExtractor;
  private final ConverterFunction<StructuredRecord, Long> timeExtractor;

  public StructuredToProducerRecordTransformer(ConfluentStreamingSinkConfig conf, Schema outputSchema) {
    this.conf = conf;
    keyExtractor = createKeyExtractor();
    valueExtractor = createValueExtractor(outputSchema);
    partitionExtractor = createPartitionExtractor();
    timeExtractor = createTimeExtractor();
  }

  public ProducerRecord<Object, Object> transform(StructuredRecord record) throws IOException {
    Object key = keyExtractor.apply(record);
    Object body = valueExtractor.apply(record);
    Integer partition = partitionExtractor.apply(record);
    Long time = timeExtractor.apply(record);
    return new ProducerRecord<>(conf.getTopic(), partition, time, key, body);
  }

  private ConverterFunction<StructuredRecord, Object> createKeyExtractor() {
    if (Strings.isNullOrEmpty(conf.getKeyField())) {
      return record -> null;
    }
    return record -> {
      Object key = record.get(conf.getKeyField());
      if (conf.getSchemaRegistryUrl() != null) {
        return key;
      }
      if (key == null) {
        return null;
      }
      if (key instanceof String) {
        return getUtf8Bytes((String) key);
      }
      if (key instanceof byte[]) {
        return (byte[]) key;
      }
      if (key instanceof ByteBuffer) {
        return ((ByteBuffer) key).array();
      }
      return getUtf8Bytes(String.valueOf(key));
    };
  }

  private ConverterFunction<StructuredRecord, Integer> createPartitionExtractor() {
    if (Strings.isNullOrEmpty(conf.getPartitionField())) {
      return record -> null;
    }
    return record -> record.<Integer>get(conf.getPartitionField());
  }

  private ConverterFunction<StructuredRecord, Long> createTimeExtractor() {
    if (Strings.isNullOrEmpty(conf.getTimeField())) {
      return record -> null;
    }
    return record -> record.<Long>get(conf.getTimeField());
  }

  private ConverterFunction<StructuredRecord, Object> createValueExtractor(Schema outputSchema) {
    if (conf.getSchemaRegistryUrl() != null) {
      Schema.Field messageField = outputSchema.getFields().get(0);
      return input -> {
        StructuredRecord messageRecord = input.get(messageField.getName());
        StructuredToAvroTransformer transformer = new StructuredToAvroTransformer(messageRecord.getSchema());
        return transformer.transform(messageRecord);
      };
    }
    if (Strings.isNullOrEmpty(conf.getFormat())) {
      Schema.Field messageField = outputSchema.getFields().get(0);
      return input -> input.get(messageField.getName());
    }
    if ("json".equalsIgnoreCase(conf.getFormat())) {
      return input -> {
        StructuredRecord.Builder recordBuilder = StructuredRecord.builder(outputSchema);
        for (Schema.Field field : outputSchema.getFields()) {
          recordBuilder.set(field.getName(), input.get(field.getName()));
        }
        StructuredRecord outputRecord = recordBuilder.build();
        return getUtf8Bytes(StructuredRecordStringConverter.toJsonString(outputRecord));
      };
    }
    if ("csv".equalsIgnoreCase(conf.getFormat())) {
      return input -> {
        List<Object> objs = getExtractedValues(input, outputSchema.getFields());
        return getUtf8Bytes(StringUtils.join(objs, ","));
      };
    }
    throw new IllegalStateException(String.format("Unsupported message format '%s'", conf.getFormat()));
  }

  private byte[] getUtf8Bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private List<Object> getExtractedValues(StructuredRecord input, List<Schema.Field> fields) {
    // Extract all values from the structured record
    List<Object> objs = Lists.newArrayList();
    for (Schema.Field field : fields) {
      objs.add(input.get(field.getName()));
    }
    return objs;
  }

  private interface ConverterFunction<T, R> extends Serializable {
    R apply(T t) throws IOException;
  }
}
