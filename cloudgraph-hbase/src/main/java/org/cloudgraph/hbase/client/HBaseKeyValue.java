package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.KeyValue;

public class HBaseKeyValue implements KeyValue {

  private org.apache.hadoop.hbase.KeyValue kv;

  @SuppressWarnings("unused")
  private HBaseKeyValue() {
  }

  public HBaseKeyValue(org.apache.hadoop.hbase.KeyValue kv) {
    super();
    this.kv = kv;
  }

  public org.apache.hadoop.hbase.KeyValue get() {
    return this.kv;
  }

  @Override
  public byte[] getRow() {
    return this.kv.getRow();
  }

  @Override
  public byte[] getFamily() {
    return this.kv.getFamily();
  }

  @Override
  public byte[] getQualifier() {
    return this.kv.getQualifier();
  }

  @Override
  public long getTimestamp() {
    return this.kv.getTimestamp();
  }

  @Override
  public byte[] getValue() {
    return this.kv.getValue();
  }

}
