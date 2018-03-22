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

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;


/**
 * A class that represents the kafka pull request.
 *
 * The class is a container for topic, leaderId, partition, uri and offset. It is
 * used in reading and writing the sequence files used for the extraction job.
 *
 */
public class KafkaRequest {

  public static final long DEFAULT_OFFSET = 0;

  private String topic = "";
  private String leaderId = "";
  private int partition = 0;

  private URI uri = null;
  private long offset = DEFAULT_OFFSET;
  private long latestOffset = -1;
  private long earliestOffset = -2;
  private long avgMsgSize = 1024;

  public KafkaRequest(String topic, String leaderId, int partition, URI brokerUri) {
    this(topic, leaderId, partition, brokerUri, DEFAULT_OFFSET, -1);
  }

  public KafkaRequest(String topic, String leaderId, int partition, URI brokerUri, long offset, long latestOffset) {
    this.topic = topic;
    this.leaderId = leaderId;
    this.uri = brokerUri;
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

  public String getLeaderId() {
    return this.leaderId;
  }

  public String getTopic() {
    return this.topic;
  }

  public URI getURI() {
    return this.uri;
  }

  public int getPartition() {
    return this.partition;
  }

  public long getOffset() {
    return this.offset;
  }

  public long getEarliestOffset() {
    if (this.earliestOffset == -2 && uri != null) {
      SimpleConsumer consumer = new SimpleConsumer(uri.getHost(), uri.getPort(), 20000, 1024 * 1024, "client");
      Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo = new HashMap<>();
      offsetInfo.put(new TopicAndPartition(topic, partition),
                     new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
      OffsetResponse response =
        consumer.getOffsetsBefore(new OffsetRequest(offsetInfo, kafka.api.OffsetRequest.CurrentVersion(), "client"));
      long[] endOffset = response.offsets(topic, partition);
      if (endOffset.length == 0) {
        throw new RuntimeException("Could not find earliest offset for topic: " + topic +
                                     " and partition: " + partition);
      }
      consumer.close();
      this.earliestOffset = endOffset[0];
      return endOffset[0];
    } else {
      return this.earliestOffset;
    }
  }

  public long getLastOffset() {
    if (this.latestOffset == -1 && uri != null)
      return getLastOffset(kafka.api.OffsetRequest.LatestTime());
    else {
      return this.latestOffset;
    }
  }

  private long getLastOffset(long time) {
    SimpleConsumer consumer = new SimpleConsumer(uri.getHost(), uri.getPort(), 60000, 1024 * 1024, "client");
    Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo = new HashMap<>();
    offsetInfo.put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(time, 1));
    OffsetResponse response =
      consumer.getOffsetsBefore(new OffsetRequest(offsetInfo, kafka.api.OffsetRequest.CurrentVersion(), "client"));
    long[] endOffset = response.offsets(topic, partition);
    consumer.close();
    if (endOffset.length == 0) {
      throw new RuntimeException("Could not find latest offset for topic: " + topic +
                                   " and partition: " + partition);
    }
    this.latestOffset = endOffset[0];
    return endOffset[0];
  }

  public long estimateDataSize() {
    long endOffset = getLastOffset();
    return (endOffset - offset) * avgMsgSize;
  }
}
