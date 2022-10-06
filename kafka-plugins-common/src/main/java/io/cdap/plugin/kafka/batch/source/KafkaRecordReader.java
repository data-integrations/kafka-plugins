/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.plugin.kafka.batch.source;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.function.Function;

/**
 * Kafka Record Reader to be used by KafkaInputFormat.
 */
public class KafkaRecordReader extends RecordReader<KafkaKey, KafkaMessage> {

  private final Function<KafkaRequest, KafkaReader> readerFunction;
  private final KafkaKey key;

  private long totalBytes;
  private KafkaReader reader;
  private long readBytes = 0;
  private KafkaMessage value;

  public KafkaRecordReader(Function<KafkaRequest, KafkaReader> readerFunction) {
    this.readerFunction = readerFunction;
    this.key = new KafkaKey();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    this.totalBytes = split.getLength();
    this.reader = readerFunction.apply(((KafkaSplit) split).getRequest());
  }

  @Override
  public void close() {
    closeReader();
  }

  @Override
  public float getProgress() {
    if (getPos() == 0) {
      return 0f;
    }

    if (getPos() >= totalBytes) {
      return 1f;
    }
    return (float) ((double) getPos() / totalBytes);
  }

  private long getPos() {
    return readBytes;
  }

  @Override
  public KafkaKey getCurrentKey() {
    return key;
  }

  @Override
  public KafkaMessage getCurrentValue() {
    return value;
  }

  @Override
  public boolean nextKeyValue() {
    if (!reader.hasNext()) {
      closeReader();
      return false;
    }

    KafkaMessage message = reader.getNext(key);

    readBytes += key.getMessageSize();
    value = message;
    return true;
  }

  private synchronized void closeReader() {
    if (reader != null) {
      try {
        reader.close();
      } catch (Exception e) {
        // not much to do here but skip the task
      } finally {
        reader = null;
      }
    }
  }
}
