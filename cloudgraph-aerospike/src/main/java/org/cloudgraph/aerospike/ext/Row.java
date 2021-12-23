package org.cloudgraph.aerospike.ext;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class Row {
  private byte[] rowKey;

  @SuppressWarnings("unused")
  private Row() {
  }

  public Row(byte[] rowKey) {
    this.rowKey = rowKey;
  }

  public Bin[] getBins() {
    Bin[] result = new Bin[size()];
    int i = 0;
    for (String key : getColumnNames()) {
      result[i] = new Bin(key, (String) null); // FIXME: no idea
      i++;
    }
    return result;
  }

  public byte[] getRow() {
    return this.rowKey;
  }

  public void addColumns(byte[] fam, byte[] qual) {
    addColumn(fam, qual);
  }

  public void addColumn(byte[] family, byte[] qualifier) {
    if (this.columns == null)
      this.columns = new HashSet<>();
    this.columns.add(Bytes.toString(qualifier));
  }

  protected Set<String> columns;

  public int size() {
    return this.columns != null ? this.columns.size() : 0;
  }

  public String[] getColumnNames() {
    String[] result = new String[size()];
    if (this.columns != null)
      this.columns.toArray(result);
    return result;
  }

  public Key getKey(TableName tableName) {
    return new Key(tableName.getNamespace(), tableName.getTableName(), Bytes.toString(this.rowKey));
  }

  @Override
  public String toString() {
    return Bytes.toString(this.rowKey);
  }

}
