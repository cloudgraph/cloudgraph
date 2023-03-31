package org.cloudgraph.rocksdb.ext;

import java.util.HashMap;
import java.util.Map;

import org.cloudgraph.common.Bytes;
import org.cloudgraph.core.client.TableName;
import org.plasma.sdo.DataType;

public abstract class RocksDBRow {
  private byte[] rowKey;

  protected Map<String, DataType> colTypeMap;

  @SuppressWarnings("unused")
  private RocksDBRow() {
  }

  protected RocksDBRow(byte[] rowKey) {
    this.rowKey = rowKey;
  }

  public abstract Object getValue(String string);

  public byte[] getRow() {
    return this.rowKey;
  }

  public void addColumns(byte[] fam, byte[] qual) {
    addColumn(fam, qual, DataType.String); // FIXME:
  }

  public void addColumns(byte[] fam, byte[] qual, DataType valueDatatype) {
    addColumn(fam, qual, valueDatatype);
  }

  public void addColumn(byte[] family, byte[] qualifier, DataType valueDatatype) {
    if (this.colTypeMap == null) {
      this.colTypeMap = new HashMap<>();
    }
    this.colTypeMap.put(Bytes.toString(qualifier), valueDatatype);
  }

  public abstract int size();

  public String[] getColumnNames() {
    if (this.colTypeMap != null) {
      String[] result = new String[this.colTypeMap.size()];
      if (this.colTypeMap != null)
        this.colTypeMap.keySet().toArray(result);
      return result;
    } else
      throw new IllegalStateException("expceted columns");
  }

  public Key getKey(TableName tableName) {
    return new Key(tableName.getNamespace(), tableName.getTableName(), this.rowKey);
  }

  @Override
  public String toString() {
    return Bytes.toString(this.rowKey);
  }

}
