package co.cask.hydrator.plugin.sink;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Class which holds values for parititon and message needs to be sent to kafka
 */
public class PartitionMessageWritable implements Writable, Configurable {
  private Text value;
  private IntWritable paritionKey;
  private Configuration conf;

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public PartitionMessageWritable() {
  }

  public PartitionMessageWritable(Text value, IntWritable paritionKey) {
    this.value = value;
    this.paritionKey = paritionKey;
  }

  public Text getValue() {
    return value;
  }

  public void setValue(Text value) {
    this.value = value;
  }

  public IntWritable getParitionKey() {
    return paritionKey;
  }

  public void setParitionKey(IntWritable paritionKey) {
    this.paritionKey = paritionKey;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    value.write(out);
    paritionKey.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value.readFields(in);
    paritionKey.readFields(in);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
