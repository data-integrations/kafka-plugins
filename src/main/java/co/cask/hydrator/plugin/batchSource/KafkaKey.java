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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * The key for the mapreduce job to pull kafka. Contains offsets and the
 * checksum.
 */
public class KafkaKey implements WritableComparable<KafkaKey> {

  private int partition = 0;
  private long beginOffset = 0;
  private long offset = 0;
  private long checksum = 0;
  private String topic = "";
  private MapWritable partitionMap = new MapWritable();

  /**
   * dummy empty constructor
   */
  public KafkaKey() {
    this("dummy", 0, 0, 0, 0);
  }

  public KafkaKey(String topic, int partition, long beginOffset, long offset) {
    this(topic, partition, beginOffset, offset, 0);
  }

  public KafkaKey(String topic, int partition, long beginOffset, long offset, long checksum) {
    this.set(topic, partition, beginOffset, offset, checksum);
  }

  public void set(String topic, int partition, long beginOffset, long offset, long checksum) {
    this.partition = partition;
    this.beginOffset = beginOffset;
    this.offset = offset;
    this.checksum = checksum;
    this.topic = topic;
  }

  public void clear() {
    partition = 0;
    beginOffset = 0;
    offset = 0;
    checksum = 0;
    topic = "";
    partitionMap = new MapWritable();
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }

  public long getMessageSize() {
    Text key = new Text("message.size");
    if (this.partitionMap.containsKey(key)) {
      return ((LongWritable) this.partitionMap.get(key)).get();
    } else {
      return 1024; //default estimated size
    }
  }

  public void setMessageSize(long messageSize) {
    Text key = new Text("message.size");
    put(key, new LongWritable(messageSize));
  }

  public void put(Writable key, Writable value) {
    this.partitionMap.put(key, value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.partition = in.readInt();
    this.beginOffset = in.readLong();
    this.offset = in.readLong();
    this.checksum = in.readLong();
    this.topic = in.readUTF();
    this.partitionMap = new MapWritable();
    this.partitionMap.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.partition);
    out.writeLong(this.beginOffset);
    out.writeLong(this.offset);
    out.writeLong(this.checksum);
    out.writeUTF(this.topic);
    this.partitionMap.write(out);
  }

  @Override
  public int compareTo(KafkaKey o) {
    int comp = Integer.compare(partition, o.partition);
    if (comp != 0) {
      return comp;
    }
    comp = Long.compare(offset, o.offset);
    if (comp != 0) {
      return comp;
    }
    return Long.compare(checksum, o.checksum);
  }
}
