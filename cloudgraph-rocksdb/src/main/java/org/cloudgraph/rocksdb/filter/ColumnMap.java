package org.cloudgraph.rocksdb.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnMap {
  private Map<String, ColumnInfo> map;

  @SuppressWarnings("unused")
  private ColumnMap() {
  }

  public ColumnMap(Map<String, ColumnInfo> map) {
    super();
    this.map = map;
  }

  public int getSize() {
    return this.map.size();
  }

  public Set<String> getColumnKeys() {
    return this.map.keySet();
  }

  @Deprecated
  public String[] getColumnNames() {
    List<String> list = new ArrayList<>();
    for (String key : this.map.keySet()) {
      list.add(key);
    }
    String[] result = new String[list.size()];
    list.toArray(result);
    return result;
  }

  public ColumnInfo getColumn(String key) {
    return this.map.get(key);
  }

}
