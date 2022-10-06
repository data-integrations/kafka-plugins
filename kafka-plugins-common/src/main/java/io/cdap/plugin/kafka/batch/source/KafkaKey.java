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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * The key for the mapreduce job to pull kafka. Contains offsets and the
 * checksum.
 */
public class KafkaKey implements WritableComparable<KafkaKey> {

  private String topic;
  private int partition;
  private long beginOffset;
  private long offset;
  private long messageSize;
  private long checksum;

  /**
   * dummy empty constructor
   */
  public KafkaKey() {
    this("dummy", 0, 0, 0, 0);
  }

  public KafkaKey(String topic, int partition, long beginOffset, long offset, long checksum) {
    this.topic = topic;
    this.partition = partition;
    set(beginOffset, offset, 1024L, checksum);
  }

  public void set(long beginOffset, long offset, long messageSize, long checksum) {
    this.beginOffset = beginOffset;
    this.offset = offset;
    this.messageSize = messageSize;
    this.checksum = checksum;
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
    return messageSize;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.topic = in.readUTF();
    this.partition = in.readInt();
    this.beginOffset = in.readLong();
    this.offset = in.readLong();
    this.messageSize = in.readLong();
    this.checksum = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.topic);
    out.writeInt(this.partition);
    out.writeLong(this.beginOffset);
    out.writeLong(this.offset);
    out.writeLong(this.messageSize);
    out.writeLong(this.checksum);
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
