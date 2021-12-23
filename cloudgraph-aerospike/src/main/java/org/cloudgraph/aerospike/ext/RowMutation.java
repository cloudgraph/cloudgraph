package org.cloudgraph.aerospike.ext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;

public class RowMutation extends Row {
  protected Map<String, BinValue> map;

  public RowMutation(byte[] rowKey) {
    super(rowKey);
  }

  @Override
  public int size() {
    return this.map != null ? this.map.size() : 0;
  }

  @Override
  public Bin[] getBins() {
    Bin[] result = new Bin[size()];
    int i = 0;
    for (String key : this.map.keySet()) {
      BinValue value = this.map.get(key);
      result[i] = value.getBin();
      i++;
    }
    return result;
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
      BinValue bv = this.map.get(Bytes.toString(qualifier));
      result.add(new Cell(bv.getValue()));
    }
    return Collections.emptyList();
  }

  public void addColumn(byte[] fam, byte[] qual, DataType valueDataType, byte[] value) {
    if (this.map == null)
      this.map = new HashMap<>();
    String binName = Bytes.toString(qual);
    if (this.map.containsKey(binName))
      throw new IllegalArgumentException("key exists: " + binName);
    Bin bin = createBin(binName, valueDataType, value);
    if (bin != null) {
      BinValue bv = new BinValue(bin, valueDataType, value);
      this.map.put(binName, bv);
    }
  }

  private Bin createBin(String name, DataType dataType, byte[] value) {
    Bin result = null;
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
      String strValue = Bytes.toString(value);
      result = new Bin(name, strValue);
      break;
    case Boolean:
      Value.UseBoolBin = true; // scary that API can access this
      boolean boolValue = Bytes.toBoolean(value);
      result = new Bin(name, boolValue);
      break;
    case Int:
      int intValue = Bytes.toInt(value);
      result = new Bin(name, intValue);
      break;
    case Long:
      long longValue = Bytes.toLong(value);
      result = new Bin(name, longValue);
      break;
    case Float:
      float floatValue = Bytes.toFloat(value);
      result = new Bin(name, floatValue);
      break;
    case Double:
      double doubleValue = Bytes.toDouble(value);
      result = new Bin(name, doubleValue);
      break;
    case Decimal:
      break;
    case Character:
      break;
    case Integer:
      break;
    case Object:
      break;
    case Short:
      break;
    case UnsignedInt:
      break;
    case UnsignedLong:
      break;
    case Byte:
    case Bytes:
    default:
      result = new Bin(name, value);
      break;
    }

    return result;
  }

  class BinValue {
    private Bin bin;
    private DataType dataType;
    private byte[] value;

    public BinValue(Bin bin, DataType dataType, byte[] value) {
      super();
      this.bin = bin;
      this.dataType = dataType;
      this.value = value;
    }

    public DataType getDataType() {
      return dataType;
    }

    public byte[] getValue() {
      return value;
    }

    public Bin getBin() {
      return bin;
    }

  }

}
