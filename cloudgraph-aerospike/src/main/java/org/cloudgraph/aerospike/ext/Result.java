package org.cloudgraph.aerospike.ext;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.aerospike.filter.ColumnInfo;
import org.cloudgraph.aerospike.filter.ColumnMap;
import org.cloudgraph.aerospike.io.KeyValue;
import org.plasma.sdo.DataType;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;

public class Result {
  private KeyInfo keyInfo;
  private Record record;
  private ColumnMap columnMap;

  @SuppressWarnings("unused")
  private Result() {
  }

  public Result(KeyInfo key, Record record, ColumnMap columnMap) {
    this.keyInfo = key;
    this.record = record;
    this.columnMap = columnMap;
  }

  public int size() {
    return this.record.bins.size();
  }

  public byte[] getValue(byte[] family, byte[] qualifier) {
    byte[] value = (byte[]) record.getValue(Bytes.toString(qualifier));
    return value;
  }

  public boolean isEmpty() {
    return this.record == null || this.record.bins.size() == 0;
  }

  public boolean containsColumn(byte[] family, byte[] qualifier) {
    return this.record.getValue(Bytes.toString(qualifier)) != null;
  }

  public byte[] getRow() {
    return Bytes.toBytes(this.keyInfo.getKey().userKey.toString()); // FIXME: no
                                                                    // // idea
  }

  public List<KeyValue> list() {
    List<KeyValue> result = new ArrayList<>();
    for (String binName : this.record.bins.keySet()) {
      if ("D|S|C1".equals(binName)) {
        int foo = 0;
        foo++;
      }
      byte[] bytesValue = null;
      ColumnInfo ci = this.columnMap.getColumn(binName);
      if (ci.hasProperty() && ci.getProperty().getType().isDataType()) {
        DataType dataType = DataType.valueOf(ci.getProperty().getType().getName());
        switch (dataType) {
        case String:
        case Strings:
        case URI:
        case Month:
        case MonthDay:
        case Day:
        case Time:
        case Year:
        case YearMonth:
        case YearMonthDay:
        case Date:
        case Duration:
        case DateTime:
          String strValue = this.record.getString(binName);
          bytesValue = Bytes.toBytes(strValue);
          break;
        case Boolean:
          boolean boolValue = this.record.getBoolean(binName);
          bytesValue = Bytes.toBytes(boolValue);
          break;
        case Int:
          int intValue = this.record.getInt(binName);
          bytesValue = Bytes.toBytes(intValue);
          break;
        case Long:
          long longValue = this.record.getLong(binName);
          bytesValue = Bytes.toBytes(longValue);
          break;
        case Float:
          float floatValue = this.record.getFloat(binName);
          bytesValue = Bytes.toBytes(floatValue);
          break;
        case Double:
          double doubleValue = this.record.getDouble(binName);
          bytesValue = Bytes.toBytes(doubleValue);
          break;
        default:
          Object value = this.record.getValue(binName);
          bytesValue = byte[].class.cast(value);
          break;
        }
      } else {
        Object value = this.record.getValue(binName); // API no specific bytes
        if (byte[].class.isInstance(value))
          bytesValue = byte[].class.cast(value);
        else if (String.class.isInstance(value))
          bytesValue = Bytes.toBytes(String.class.cast(value));
        else
          throw new IllegalStateException("unexpected " + value.getClass().getName());
      }
      KeyValue kv = new KeyValue(this.keyInfo.getKey().digest, Bytes.toBytes(this.keyInfo
          .getFamily()), Bytes.toBytes(binName), bytesValue);
      result.add(kv);
    }
    return result;
  }

}
