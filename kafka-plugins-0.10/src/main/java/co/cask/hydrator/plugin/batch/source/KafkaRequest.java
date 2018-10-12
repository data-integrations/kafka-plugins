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

import com.google.common.collect.ImmutableMap;

import java.util.Map;


/**
 * A class that represents the kafka pull request.
 * <p>
 * The class is a container for topic, leaderId, partition, uri and offset. It is
 * used in reading and writing the sequence files used for the extraction job.
 */
public class KafkaRequest {

  public static final long DEFAULT_OFFSET = 0;

  private Map<String, String> conf;
  private String topic = "";
  private int partition = 0;

  private long offset = DEFAULT_OFFSET;
  private long latestOffset = -1;
  private long earliestOffset = -2;
  private long avgMsgSize = 1024;

  public KafkaRequest(Map<String, String> conf, String topic, int partition) {
    this(conf, topic, partition, DEFAULT_OFFSET, -1);
  }

  public KafkaRequest(Map<String, String> conf, String topic, int partition, long offset, long latestOffset) {
    this.conf = ImmutableMap.copyOf(conf);
    this.topic = topic;
    this.partition = partition;
    this.latestOffset = latestOffset;
    setOffset(offset);
  }

  public void setLatestOffset(long latestOffset) {
    this.latestOffset = latestOffset;
  }

  public void setEarliestOffset(long earliestOffset) {
    this.earliestOffset = earliestOffset;
  }

  public void setAvgMsgSize(long size) {
    this.avgMsgSize = size;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  public String getTopic() {
    return this.topic;
  }

  public int getPartition() {
    return this.partition;
  }

  public long getOffset() {
    return this.offset;
  }

  public long getEarliestOffset() {
    return this.earliestOffset;
  }

  public long getLastOffset() {
    return this.latestOffset;
  }

  public long estimateDataSize() {
    long endOffset = getLastOffset();
    return (endOffset - offset) * avgMsgSize;
  }
}
