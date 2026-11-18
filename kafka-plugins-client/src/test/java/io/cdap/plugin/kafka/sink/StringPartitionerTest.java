/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.kafka.sink;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link StringPartitioner} to ensure partition assignments remain identical
 * to Guava 13's {@code Hashing.md5().hashString(CharSequence)} (unencoded UTF-16LE chars).
 */
public class StringPartitionerTest {

  @Test
  public void testFixedKeyToPartitionMapping() throws Exception {
    String topic = "test-topic";
    int numPartitions = 16;
    List<PartitionInfo> partitions = new ArrayList<>(numPartitions);
    Node node = new Node(0, "localhost", 9092);
    Node[] nodes = new Node[]{node};
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(new PartitionInfo(topic, i, node, nodes, nodes));
    }
    Cluster cluster = new Cluster("cluster-id", Collections.singletonList(node), partitions,
                                  Collections.emptySet(), Collections.emptySet());

    StringPartitioner partitioner = new StringPartitioner();
    Assert.assertEquals(14, partitioner.partition(topic, "key-1", null, null, null, cluster));
    Assert.assertEquals(15, partitioner.partition(topic, "key-2", null, null, null, cluster));
    Assert.assertEquals(7, partitioner.partition(topic, "user-12345", null, null, null, cluster));
    Assert.assertEquals(4, partitioner.partition(topic, "日本語-key", null, null, null, cluster));

    String[] keys = new String[]{"", "a", "key-1", "key-2", "user-12345", "日本語-key"};
    for (String key : keys) {
      int expected = legacyGuava13HashStringPartition(key, numPartitions);
      Assert.assertEquals("Partition mismatch for key: " + key,
                          expected, partitioner.partition(topic, key, null, null, null, cluster));
    }
  }

  private static int legacyGuava13HashStringPartition(CharSequence input, int numPartitions) throws Exception {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    ByteBuffer buf = ByteBuffer.allocate(input.length() * 2).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < input.length(); i++) {
      buf.putChar(input.charAt(i));
    }
    byte[] digest = md5.digest(buf.array());
    int hashAsInt = ByteBuffer.wrap(digest, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    return Math.abs(hashAsInt) % numPartitions;
  }
}
