package org.cloudgraph.rocksdb.filter;

import java.util.Map;
import java.util.Set;

public class MultiColumnPrefixFilter extends Filter {
  private ColumnMap map;

  public MultiColumnPrefixFilter(Map<String, ColumnInfo> map) {
    this.map = new ColumnMap(map);
  }

  public int getSize() {
    return map.getSize();
  }

  public Set<String> getColumnKeys() {
    return map.getColumnKeys();
  }

  public String[] getColumnNames() {
    return map.getColumnNames();
  }

  public ColumnInfo getColumn(String key) {
    return map.getColumn(key);
  }

}
