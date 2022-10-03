package org.cloudgraph.rocksdb.ext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.plasma.sdo.DataType;

public abstract class Row {
  private byte[] rowKey;

  // protected Map<String, DataType> colTypeMap;

  @SuppressWarnings("unused")
  private Row() {
  }

  protected Row(byte[] rowKey) {
    this.rowKey = rowKey;
  }

  public abstract Object getValue(String string);

  public byte[] getRow() {
    return this.rowKey;
  }

  // public void addColumns(byte[] fam, byte[] qual) {
  // addColumn(fam, qual, DataType.String); // FIXME:
  // }
  //
  // public void addColumns(byte[] fam, byte[] qual, DataType valueDatatype) {
  // addColumn(fam, qual, valueDatatype);
  // }

  // public abstract void addColumn(byte[] family, byte[] qualifier, DataType
  // valueDatatype);
  // {
  // if (this.colTypeMap == null)
  // this.colTypeMap = new HashMap<>();
  // this.colTypeMap.put(Bytes.toString(qualifier), valueDatatype);
  // }

  public abstract int size();

  // {
  // return this.getColumns().length;
  // }

  public abstract String[] getColumnNames();

  // {
  // if (this.colTypeMap != null) {
  // String[] result = new String[this.colTypeMap.size()];
  // if (this.colTypeMap != null)
  // this.colTypeMap.keySet().toArray(result);
  // return result;
  // } else
  // throw new IllegalStateException("expceted columns");
  // }

  public Key getKey(TableName tableName) {
    return new Key(tableName.getNamespace(), tableName.getTableName(), this.rowKey);
  }

  @Override
  public String toString() {
    return Bytes.toString(this.rowKey);
  }

}
