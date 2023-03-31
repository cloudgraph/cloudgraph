package org.cloudgraph.hbase.client;

import java.util.Map;
import java.util.NavigableSet;

import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.client.Get;

public class HBaseGet implements Get {
  private org.apache.hadoop.hbase.client.Get get;

  @SuppressWarnings("unused")
  private HBaseGet() {
  }

  public HBaseGet(org.apache.hadoop.hbase.client.Get get) {
    super();
    this.get = get;
  }

  public HBaseGet(byte[] rowKey) {
    this(new org.apache.hadoop.hbase.client.Get(rowKey));
  }

  public org.apache.hadoop.hbase.client.Get get() {
    return this.get;
  }

  @Override
  public byte[] getRow() {
    return this.get.getRow();
  }

  @Override
  public void addColumn(byte[] family, byte[] qualifier) {
    this.get.addColumn(family, qualifier);
  }

  @Override
  public void setFilter(Filter filter) {
    this.get.setFilter(HBaseFilter.class.cast(filter).get());
  }

  @Override
  public Filter getFilter() {
    org.apache.hadoop.hbase.filter.Filter filter = this.get.getFilter();
    if (filter != null) {
      return new HBaseFilter(filter);
    }
    return null;
  }

  @Override
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return this.get.getFamilyMap();
  }

}
