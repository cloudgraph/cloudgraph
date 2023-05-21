package org.cloudgraph.rocksdb.client;

import java.util.Map;
import java.util.NavigableSet;

import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.client.Get;
import org.plasma.sdo.DataType;

public class RocksDBGet extends RocksDBRow implements Get {

  private Filter columnFilter;
  protected Map<String, DataType> colTypeMap;

  public RocksDBGet(byte[] rowKey) {
    super(rowKey);
  }

  public RocksDBGet(byte[] rowKey, Filter columnFilter) {
    this(rowKey);
    this.columnFilter = columnFilter;
  }

  @Override
  public String[] getColumnNames() {
    if (this.columnFilter != null) {
      throw new RuntimeException("not implemented");

    }
    return null;
  }

  @Override
  public int size() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public Object getValue(String string) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public void addColumn(byte[] family, byte[] qualifier) {
    super.addColumn(family, qualifier, DataType.String); // FIXME:
  }

  @Override
  public void setFilter(org.cloudgraph.core.client.Filter filter) {
    this.columnFilter = filter;

  }

  @Override
  public org.cloudgraph.core.client.Filter getFilter() {
    return this.columnFilter;
  }

  @Override
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    throw new RuntimeException("not implemented");
  }

}
