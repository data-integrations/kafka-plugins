package co.cask.hydrator.plugin.sink;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * String partitioner for kafka
 */
@SuppressWarnings("UnusedDeclaration")
public final class StringPartitioner implements Partitioner {
  private final int numPartitions;

  public StringPartitioner(VerifiableProperties props) {
    this.numPartitions = Integer.parseInt(props.getProperty("numOfPartitions"));
    Preconditions.checkArgument(this.numPartitions > 0,
                                "numPartitions should be at least 1. Got %s", this.numPartitions);
  }

  @Override
  public int partition(Object key, int numPartitions) {
    return Math.abs(Hashing.md5().hashString(key.toString()).asInt()) % this.numPartitions;
  }
}
