package org.cloudgraph.rocksdb.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.Bytes;
import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.KeyValue;
import org.cloudgraph.core.client.Result;
import org.cloudgraph.rocksdb.io.RocksDBKeyValue;

public class RocksDBResult extends RocksDBRow implements Result {
  private static Log log = LogFactory.getLog(RocksDBResult.class);
  private KeyInfo keyInfo;
  private Column[] columns;
  private Map<String, Column> columnMap;

  public RocksDBResult(byte[] rowKey) {
    super(rowKey);
  }

  public RocksDBResult(KeyInfo key, Column[] columns) {
    super(key.getKey().getData());
    this.keyInfo = key;
    this.columns = columns;
    this.columnMap = new HashMap<>();
    for (Column col : columns) {
      if (this.columnMap.containsKey(col.getName()))
        log.warn("found existing column key: " + col.getName());
      this.columnMap.put(col.getName(), col);
    }
  }

  public byte[] getValue(byte[] family, byte[] qualifier) {
    if (!isEmpty()) {
      String name = Bytes.toString(qualifier);
      Column column = this.columnMap.get(name);
      byte[] value = null;
      if (column != null) {
        value = column.getData();
      }
      return value;
    }

    return null;
  }

  public boolean isEmpty() {
    return this.size() == 0;
  }

  public boolean containsColumn(byte[] family, byte[] qualifier) {
    return this.getValue(family, qualifier) != null;
  }

  public byte[] getRow() {
    return super.getRow();
  }

  public Column[] getColumns() {
    return this.columns;
  }

  @Override
  public int size() {
    return columnMap.size();
  }

  @Override
  public String[] getColumnNames() {
    String[] result = new String[columnMap.size()];
    int i = 0;
    for (String key : columnMap.keySet()) {
      result[i] = key;
      i++;
    }
    return result;
  }

  @Override
  public Object getValue(String name) {
    Column column = this.columnMap.get(name);
    if (column != null) {
      if (log.isDebugEnabled())
        log.debug("get column: " + column.toString());
      byte[] bytesValue = column.getData();
      return bytesValue;
    }
    return null;
  }

  public List<KeyValue> list() {
    List<KeyValue> result = new ArrayList<>();
    if (!isEmpty()) {
      for (Column column : getColumns()) {
        if (log.isDebugEnabled())
          log.debug("list column: " + column.toString());
        byte[] bytesValue = column.getData();
        KeyValue kv = new RocksDBKeyValue(this.keyInfo.getKey().getData(),
            Bytes.toBytes(this.keyInfo.getFamily()), Bytes.toBytes(column.getName()), bytesValue);
        result.add(kv);
      }
    }
    return result;
  }

  @Override
  public Cell[] rawCells() {
    Cell[] result = new Cell[0];
    if (!isEmpty()) {
      Column[] columns = getColumns();
      result = new Cell[columns.length];
      int i = 0;
      for (Column column : columns) {
        if (log.isDebugEnabled())
          log.debug("raw column: " + column.toString());
        byte[] bytesValue = column.getData();
        Cell cell = new RocksDBCell(/* this.keyInfo.getKey().getData(), */
        Bytes.toBytes(this.keyInfo.getFamily()), Bytes.toBytes(column.getName()), bytesValue);
        result[i] = cell;
        i++;
      }
    }
    return result;
  }

  // private byte[] toBytes(ColumnInfo ci, String name) {
  // byte[] result = null;
  // if (ci.hasProperty() && ci.getProperty().getType().isDataType()) {
  // DataType dataType = DataType.valueOf(ci.getProperty().getType().getName());
  // switch (dataType) {
  // case String:
  // case Strings:
  // case URI:
  // case Month:
  // case MonthDay:
  // case Day:
  // case Time:
  // case Year:
  // case YearMonth:
  // case YearMonthDay:
  // case Date:
  // case Duration:
  // case DateTime:
  // case Boolean:
  // case Int:
  // case Long:
  // case Float:
  // case Double:
  // default:
  // Object value = this.getValue(name);
  // result = byte[].class.cast(value);
  // break;
  // }
  // } else {
  // Object value = this.getValue(name); // API no specific bytes
  // if (value != null) {
  // if (byte[].class.isInstance(value))
  // result = byte[].class.cast(value);
  // else if (String.class.isInstance(value))
  // result = Bytes.toBytes(String.class.cast(value));
  // else
  // throw new IllegalStateException("unexpected " +
  // value.getClass().getName());
  // }
  // }
  // return result;
  // }

}
