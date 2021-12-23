package org.cloudgraph.aerospike.ext;

import org.cloudgraph.aerospike.filter.Filter;

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
    String[] result = super.getColumnNames();
    if (this.columnFilter != null) {
      result = this.columnFilter.getColumnNames();
    } else {
      result = super.getColumnNames();
    }
    return result;
  }
}
