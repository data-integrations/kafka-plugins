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

package io.cdap.plugin.source;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.common.KafkaHelpers;
import kafka.api.OffsetRequest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Util method for {@link KafkaStreamingSource}.
 *
 * This class contains methods for {@link KafkaStreamingSource} that require spark classes because during validation
 * spark classes are not available. Refer CDAP-15912 for more information.
 */
final class KafkaStreamingSourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamingSourceUtil.class);

  /**
   * Returns {@link JavaDStream} for {@link KafkaStreamingSource}.
   *
   * @param context streaming context
   * @param conf kafka conf
   * @param collector failure collector
   */
  static JavaDStream<StructuredRecord> getStructuredRecordJavaDStream(
    StreamingContext context, KafkaConfig conf, FailureCollector collector) {
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getBrokers());
    // Spark saves the offsets in checkpoints, no need for Kafka to save them
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    kafkaParams.put("key.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    kafkaParams.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    KafkaHelpers.setupKerberosLogin(kafkaParams, conf.getPrincipal(), conf.getKeytabLocation());
    // Create a unique string for the group.id using the pipeline name and the topic.
    // group.id is a Kafka consumer property that uniquely identifies the group of
    // consumer processes to which this consumer belongs.
    kafkaParams.put("group.id", Joiner.on("-").join(context.getPipelineName().length(), conf.getTopic().length(),
                                                    context.getPipelineName(), conf.getTopic()));
    kafkaParams.putAll(conf.getKafkaProperties());

    Properties properties = new Properties();
    properties.putAll(kafkaParams);
    // change the request timeout to fetch the metadata to be 15 seconds or 1 second greater than session time out ms,
    // since this config has to be greater than the session time out, which is by default 10 seconds
    // the KafkaConsumer at runtime should still use the default timeout 305 seconds or whataver the user provides in
    // kafkaConf
    int requestTimeout = 15 * 1000;
    if (conf.getKafkaProperties().containsKey(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG)) {
      requestTimeout =
        Math.max(requestTimeout,
                 Integer.valueOf(conf.getKafkaProperties().get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) + 1000));
    }
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
    try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(),
                                                                 new ByteArrayDeserializer())) {
      Map<TopicPartition, Long> offsets = conf.getInitialPartitionOffsets(
        getPartitions(consumer, conf, collector), collector);
      collector.getOrThrowException();

      // KafkaUtils doesn't understand -1 and -2 as smallest offset and latest offset.
      // so we have to replace them with the actual smallest and latest
      List<TopicPartition> earliestOffsetRequest = new ArrayList<>();
      List<TopicPartition> latestOffsetRequest = new ArrayList<>();
      for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
        TopicPartition topicAndPartition = entry.getKey();
        Long offset = entry.getValue();
        if (offset == OffsetRequest.EarliestTime()) {
          earliestOffsetRequest.add(topicAndPartition);
        } else if (offset == OffsetRequest.LatestTime()) {
          latestOffsetRequest.add(topicAndPartition);
        }
      }

      Set<TopicPartition> allOffsetRequest =
        Sets.newHashSet(Iterables.concat(earliestOffsetRequest, latestOffsetRequest));
      Map<TopicPartition, Long> offsetsFound = new HashMap<>();
      offsetsFound.putAll(KafkaHelpers.getEarliestOffsets(consumer, earliestOffsetRequest));
      offsetsFound.putAll(KafkaHelpers.getLatestOffsets(consumer, latestOffsetRequest));
      for (TopicPartition topicAndPartition : allOffsetRequest) {
        offsets.put(topicAndPartition, offsetsFound.get(topicAndPartition));
      }

      Set<TopicPartition> missingOffsets = Sets.difference(allOffsetRequest, offsetsFound.keySet());
      if (!missingOffsets.isEmpty()) {
        throw new IllegalStateException(String.format(
          "Could not find offsets for %s. Please check all brokers were included in the broker list.", missingOffsets));
      }
      LOG.info("Using initial offsets {}", offsets);

      return KafkaUtils.createDirectStream(
        context.getSparkStreamingContext(), LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<byte[], byte[]>Subscribe(Collections.singleton(conf.getTopic()), kafkaParams, offsets)
      ).transform(new RecordTransform(conf));
    } catch (KafkaException e) {
      LOG.error("Exception occurred while trying to read from kafka topic: {}", e.getMessage());
      LOG.error("Please verify that the hostname/IPAddress of the kafka server is correct and that it is running.");
      throw e;
    }
  }

  /**
   * Applies the format function to each rdd.
   */
  private static class RecordTransform
    implements Function2<JavaRDD<ConsumerRecord<byte[], byte[]>>, Time, JavaRDD<StructuredRecord>> {

    private final KafkaConfig conf;

    RecordTransform(KafkaConfig conf) {
      this.conf = conf;
    }

    @Override
    public JavaRDD<StructuredRecord> call(JavaRDD<ConsumerRecord<byte[], byte[]>> input, Time batchTime) {
      Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> recordFunction = conf.getFormat() == null ?
        new BytesFunction(batchTime.milliseconds(), conf) :
        new FormatFunction(batchTime.milliseconds(), conf);
      return input.map(recordFunction);
    }
  }

  private static Set<Integer> getPartitions(Consumer<byte[], byte[]> consumer, KafkaConfig conf,
                                            FailureCollector collector) {
    Set<Integer> partitions = conf.getPartitions(collector);
    collector.getOrThrowException();

    if (!partitions.isEmpty()) {
      return partitions;
    }

    partitions = new HashSet<>();
    for (PartitionInfo partitionInfo : consumer.partitionsFor(conf.getTopic())) {
      partitions.add(partitionInfo.partition());
    }
    return partitions;
  }

  /**
   * Common logic for transforming kafka key, message, partition, and offset into a structured record.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private abstract static class BaseFunction implements Function<ConsumerRecord<byte[], byte[]>, StructuredRecord> {
    private final long ts;
    protected final KafkaConfig conf;
    private transient String messageField;
    private transient String timeField;
    private transient String keyField;
    private transient String partitionField;
    private transient String offsetField;
    private transient Schema schema;

    BaseFunction(long ts, KafkaConfig conf) {
      this.ts = ts;
      this.conf = conf;
    }

    @Override
    public StructuredRecord call(ConsumerRecord<byte[], byte[]> in) throws Exception {
      // first time this was called, initialize schema and time, key, and message fields.
      if (schema == null) {
        schema = conf.getSchema();
        timeField = conf.getTimeField();
        keyField = conf.getKeyField();
        partitionField = conf.getPartitionField();
        offsetField = conf.getOffsetField();
        for (Schema.Field field : schema.getFields()) {
          String name = field.getName();
          if (!name.equals(timeField) && !name.equals(keyField)) {
            messageField = name;
            break;
          }
        }
      }

      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      if (timeField != null) {
        builder.set(timeField, ts);
      }
      if (keyField != null) {
        builder.set(keyField, in.key());
      }
      if (partitionField != null) {
        builder.set(partitionField, in.partition());
      }
      if (offsetField != null) {
        builder.set(offsetField, in.offset());
      }
      addMessage(builder, messageField, in.value());
      return builder.build();
    }

    protected abstract void addMessage(StructuredRecord.Builder builder, String messageField,
                                       byte[] message) throws Exception;
  }

  /**
   * Transforms kafka key and message into a structured record when message format is not given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class BytesFunction extends BaseFunction {

    BytesFunction(long ts, KafkaConfig conf) {
      super(ts, conf);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, String messageField, byte[] message) {
      builder.set(messageField, message);
    }
  }

  /**
   * Transforms kafka key and message into a structured record when message format and schema are given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private static class FormatFunction extends BaseFunction {
    private transient RecordFormat<ByteBuffer, StructuredRecord> recordFormat;

    FormatFunction(long ts, KafkaConfig conf) {
      super(ts, conf);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, String messageField, byte[] message) throws Exception {
      // first time this was called, initialize record format
      if (recordFormat == null) {
        Schema messageSchema = conf.getMessageSchema();
        FormatSpecification spec =
          new FormatSpecification(conf.getFormat(), messageSchema, new HashMap<>());
        recordFormat = RecordFormats.createInitializedFormat(spec);
      }

      StructuredRecord messageRecord = recordFormat.read(ByteBuffer.wrap(message));
      for (Schema.Field field : messageRecord.getSchema().getFields()) {
        String fieldName = field.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
    }
  }

  private KafkaStreamingSourceUtil() {
    // no-op
  }
}
