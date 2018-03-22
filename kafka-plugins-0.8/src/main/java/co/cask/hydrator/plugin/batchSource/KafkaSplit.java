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


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
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
      String topic = in.readUTF();
      String leaderId = in.readUTF();
      String str = in.readUTF();
      URI uri = null;
      if (!str.isEmpty())
        try {
          uri = new URI(str);
        } catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      int partition = in.readInt();
      long offset = in.readLong();
      long latestOffset = in.readLong();
      request = new KafkaRequest(topic, leaderId, partition, uri, offset, latestOffset);
      length = request.estimateDataSize();
  }

  @Override
  public void write(DataOutput out) throws IOException {
      out.writeUTF(request.getTopic());
      out.writeUTF(request.getLeaderId());
      if (request.getURI() != null)
        out.writeUTF(request.getURI().toString());
      else
        out.writeUTF("");
      out.writeInt(request.getPartition());
      out.writeLong(request.getOffset());
      out.writeLong(request.getLastOffset());
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
}
