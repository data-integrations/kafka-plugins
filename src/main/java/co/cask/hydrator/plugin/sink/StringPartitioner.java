package co.cask.hydrator.plugin.sink;

import com.google.common.hash.Hashing;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * String partitioner for kafka
 */
@SuppressWarnings("UnusedDeclaration")
public final class StringPartitioner implements Partitioner {

  public StringPartitioner(VerifiableProperties props) {

  }

  @Override
  public int partition(Object key, int numPartitions) {
    return Math.abs(Hashing.md5().hashString(key.toString()).asInt()) % numPartitions;
  }
}
