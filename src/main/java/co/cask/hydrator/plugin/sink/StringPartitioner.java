package co.cask.hydrator.plugin.sink;

import com.google.common.hash.Hashing;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * String partitioner for kafka
 */
@SuppressWarnings("UnusedDeclaration")
public final class StringPartitioner implements Partitioner {

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();
    return Math.abs(Hashing.md5().hashString(key.toString()).asInt()) % numPartitions;
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(Map<String, ?> map) {
  }
}
