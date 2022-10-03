package org.cloudgraph.rocksdb.ext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;

public abstract class RowMutation extends Row {
  private static Log log = LogFactory.getLog(RowMutation.class);
  private static Column[] EMPTY_COLUMN_ARRAY = new Column[0];

  protected enum MutationType {
    PUT, DEL, INCREMENT;
  }

  protected Map<String, ColumnValue> map;
  protected MutationType mutationType;

  public RowMutation(byte[] rowKey, MutationType mutationType) {
    super(rowKey);
    this.mutationType = mutationType;
  }

  public MutationType getMutationType() {
    return mutationType;
  }

  @Override
  public int size() {
    return this.map != null ? this.map.size() : 0;
  }

  public Column[] getColumns() {
    if (this.map != null) {
      Column[] result = new Column[size()];
      int i = 0;
      for (String key : this.map.keySet()) {
        ColumnValue value = this.map.get(key);
        result[i] = value.getBin();
        i++;
      }
      return result;
    } else {
      return EMPTY_COLUMN_ARRAY;
    }
  }

  @Override
  public Object getValue(String name) {
    ColumnValue cv = this.map.get(name);
    if (cv != null) {
      cv.getValue();
    }
    return null;
  }

  @Override
  public String[] getColumnNames() {
    String[] result = new String[size()];
    this.map.keySet().toArray(result);
    return result;
  }

  public boolean has(byte[] family, byte[] qualifier) {
    if (map != null) {
      return this.map.containsKey(Bytes.toString(qualifier));
    }
    return false;
  }

  public List<Cell> get(byte[] family, byte[] qualifier) {
    if (map != null) {
      List<Cell> result = new ArrayList<>();
      ColumnValue bv = this.map.get(Bytes.toString(qualifier));
      result.add(new Cell(bv.getValue()));
    }
    return Collections.emptyList();
  }

  public void addColumn(byte[] fam, byte[] qual, DataType valueDataType, byte[] value) {
    // super.addColumn(fam, qual, valueDataType);
    if (this.map == null)
      this.map = new HashMap<>();
    String qualifier = Bytes.toString(qual);
    if (qualifier.endsWith("UU@5")) {
      int foo = 0;
      foo++;
    }
    if (this.map.containsKey(qualifier)) {
      if (this.map.size() > 0) {
        if (this.map.containsKey(qualifier)) {
          ColumnValue existing = this.map.get(qualifier);
          org.cloudgraph.rocksdb.ext.Bytes existingBytes = new org.cloudgraph.rocksdb.ext.Bytes(
              existing.getValue());
          org.cloudgraph.rocksdb.ext.Bytes currBytes = new org.cloudgraph.rocksdb.ext.Bytes(value);
          int compare = currBytes.compareTo(existingBytes);
          if (compare != 0) {
            throw new IllegalArgumentException("attempt to overwrite qualifier '" + qualifier
                + "' old value: '" + org.cloudgraph.common.Bytes.toString(existing.getValue())
                + "' with: '" + org.cloudgraph.common.Bytes.toString(value) + "'");
          } else {
            log.warn("qualifier '" + qualifier + "' overwritten same value - ignoring");
          }
        }
      }
    }
    Column column = createColumn(qualifier, valueDataType, value);
    ColumnValue columnValue = new ColumnValue(column, valueDataType, value);
    this.map.put(qualifier, columnValue);
  }

  private Column createColumn(String name, DataType dataType, byte[] value) {
    Column result = null;
    switch (dataType) {
    default:
      result = new Column(name, value);
      break;
    }

    return result;
  }

  class ColumnValue {
    private Column column;
    private DataType dataType;
    private byte[] value;

    public ColumnValue(Column column, DataType dataType, byte[] value) {
      super();
      this.column = column;
      this.dataType = dataType;
      this.value = value;
    }

    public DataType getDataType() {
      return dataType;
    }

    public byte[] getValue() {
      return value;
    }

    public Column getBin() {
      return column;
    }

  }

}
