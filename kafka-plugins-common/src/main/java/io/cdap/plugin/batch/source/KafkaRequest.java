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

package io.cdap.plugin.batch.source;

import java.util.Collections;
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

  private final String topic;
  private final int partition;
  private final Map<String, String> conf;

  private final long startOffset;
  private final long endOffset;

  private final long averageMessageSize;

  public KafkaRequest(String topic, int partition, Map<String, String> conf, long startOffset, long endOffset) {
    this(topic, partition, conf, startOffset, endOffset, 1024);
  }

  public KafkaRequest(String topic, int partition, Map<String, String> conf,
                      long startOffset, long endOffset, long averageMessageSize) {
    this.topic = topic;
    this.partition = partition;
    this.conf = Collections.unmodifiableMap(new HashMap<>(conf));
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.averageMessageSize = averageMessageSize;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getEndOffset() {
    return endOffset;
  }

  public long getAverageMessageSize() {
    return averageMessageSize;
  }

  public long estimateDataSize() {
    return (getEndOffset() - getStartOffset()) * averageMessageSize;
  }
}
