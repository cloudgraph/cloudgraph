package org.cloudgraph.rocksdb.ext;

import org.cloudgraph.rocksdb.filter.Filter;

public class Get extends Row {

  private Filter columnFilter;

  public Get(byte[] rowKey) {
    super(rowKey);
  }

  public Get(byte[] rowKey, Filter columnFilter) {
    this(rowKey);
    this.columnFilter = columnFilter;
  }

  public void setColumnFilter(Filter columnFilter) {
    this.columnFilter = columnFilter;
  }

  public Filter getColumnFilter() {
    return columnFilter;
  }

  @Override
  public String[] getColumnNames() {
    if (this.columnFilter != null) {

    }
    return null;
  }

  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Object getValue(String string) {
    throw new RuntimeException("not implemented");
  }

}
