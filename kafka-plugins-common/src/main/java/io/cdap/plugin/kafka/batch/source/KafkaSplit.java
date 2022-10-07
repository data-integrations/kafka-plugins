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


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka split
 */
public class KafkaSplit extends InputSplit implements Writable {

  private KafkaRequest request;

  @SuppressWarnings("unused")
  public KafkaSplit() {
    // For serialization
  }

  public KafkaSplit(KafkaRequest request) {
    this.request = request;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String topic = in.readUTF();
    int partition = in.readInt();

    // Read config map
    int size = in.readInt();
    Map<String, String> conf = new HashMap<>();
    for (int i = 0; i < size; i++) {
      conf.put(in.readUTF(), in.readUTF());
    }

    long startOffset = in.readLong();
    long endOffset = in.readLong();
    long averageMessageSize = in.readLong();
    request = new KafkaRequest(topic, partition, conf, startOffset, endOffset, averageMessageSize);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(request.getTopic());
    out.writeInt(request.getPartition());

    // Write config map
    Map<String, String> conf = request.getConf();
    out.writeInt(conf.size());
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }

    out.writeLong(request.getStartOffset());
    out.writeLong(request.getEndOffset());
    out.writeLong(request.getAverageMessageSize());
  }

  @Override
  public long getLength() {
    return request.estimateDataSize();
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  public KafkaRequest getRequest() {
    return request;
  }
}
