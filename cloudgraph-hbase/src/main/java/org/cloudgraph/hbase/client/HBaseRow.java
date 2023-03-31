package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.Row;

public abstract class HBaseRow implements Row {
  private org.apache.hadoop.hbase.client.Row row;

  @SuppressWarnings("unused")
  private HBaseRow() {
  }

  public HBaseRow(org.apache.hadoop.hbase.client.Row row) {
    super();
    this.row = row;
  }

  public org.apache.hadoop.hbase.client.Row get() {
    return row;
  }

  @Override
  public byte[] getRow() {
    return this.row.getRow();
  }

}
