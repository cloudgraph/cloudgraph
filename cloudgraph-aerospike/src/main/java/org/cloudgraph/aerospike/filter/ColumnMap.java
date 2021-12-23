package org.cloudgraph.aerospike.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ColumnMap {
  private Map<String, ColumnInfo> map;

  protected ColumnMap(Map<String, ColumnInfo> map) {
    super();
    this.map = map;
  }

  public int getSize() {
    return this.map.size();
  }

  public Set<String> getColumnKeys() {
    return this.map.keySet();
  }

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
