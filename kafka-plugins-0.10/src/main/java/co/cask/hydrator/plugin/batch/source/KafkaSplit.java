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

package co.cask.hydrator.plugin.batch.source;


import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Kafka split
 */
public class KafkaSplit extends InputSplit implements Writable {
  private KafkaRequest request;
  private long length = 0;

  public KafkaSplit() {
  }

  public KafkaSplit(KafkaRequest request) {
    this.request = request;
    length = request.estimateDataSize();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    MapWritable mapWritable = new MapWritable();
    mapWritable.readFields(in);
    String topic = in.readUTF();
    int partition = in.readInt();
    long offset = in.readLong();
    long latestOffset = in.readLong();
    long earliestOffset = in.readLong();
    Map<String, String> conf = writableToConf(mapWritable);
    request = new KafkaRequest(conf, topic, partition, offset, latestOffset);
    request.setEarliestOffset(earliestOffset);
    length = request.estimateDataSize();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    MapWritable conf = confToWritable(request.getConf());
    conf.write(out);
    out.writeUTF(request.getTopic());
      out.writeInt(request.getPartition());
      out.writeLong(request.getOffset());
      out.writeLong(request.getLastOffset());
      out.writeLong(request.getEarliestOffset());
  }

  @Override
  public long getLength() throws IOException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[] {};
  }

  @Nullable
  public KafkaRequest popRequest() {
    KafkaRequest result = request;
    request = null;
    return result;
  }

  private MapWritable confToWritable(Map<String, String> conf) {
    MapWritable mapWritable = new MapWritable();
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      mapWritable.put(new Text(entry.getKey()), new Text(entry.getValue()));
    }
    return mapWritable;
  }

  private Map<String, String> writableToConf(MapWritable mapWritable) {
    Map<String, String> conf = new HashMap<>();
    for (Map.Entry<Writable, Writable> entry : mapWritable.entrySet()) {
      conf.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return conf;
  }
}
