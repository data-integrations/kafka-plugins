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

package co.cask.hydrator.plugin.batchSource;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Kafka Record Reader to be used by {@link KafkaInputFormat}.
 */
public class KafkaRecordReader extends RecordReader<KafkaKey, KafkaMessage> {

  private KafkaSplit split;
  private long totalBytes;
  private KafkaReader reader;
  private long readBytes = 0;
  private final KafkaKey key = new KafkaKey();
  private KafkaMessage value;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    this.split = (KafkaSplit) split;
    this.totalBytes = this.split.getLength();
  }

  @Override
  public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    if (getPos() == 0) {
      return 0f;
    }

    if (getPos() >= totalBytes) {
      return 1f;
    }
    return (float) ((double) getPos() / totalBytes);
  }

  private long getPos() throws IOException {
    return readBytes;
  }

  @Override
  public KafkaKey getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public KafkaMessage getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (reader == null || !reader.hasNext()) {
      KafkaRequest request = split.popRequest();
      if (request == null) {
        return false;
      }

      key.set(request.getTopic(), request.getPartition(), request.getOffset(),
              request.getOffset(), 0);
      value = null;

      if (reader != null) {
        closeReader();
      }
      reader = new KafkaReader(request);
    }
    KafkaMessage message;
    if ((message = reader.getNext(key)) != null) {
      readBytes += key.getMessageSize();
      value = message;
      return true;
    }
    return false;
  }


  private void closeReader() throws IOException {
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
