package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.Increment;

public class HBaseIncrement extends HBaseMutation implements Increment {

  private org.apache.hadoop.hbase.client.Increment increment;

  public HBaseIncrement(org.apache.hadoop.hbase.client.Increment increment) {
    super(increment);
    this.increment = increment;
  }

  public HBaseIncrement(byte[] rowKey) {
    this(new org.apache.hadoop.hbase.client.Increment(rowKey));
  }

  public org.apache.hadoop.hbase.client.Increment get() {
    return this.increment;
  }

  @Override
  public void addColumn(byte[] fam, byte[] qual, long value) {
    this.increment.addColumn(fam, qual, value);
  }

}
