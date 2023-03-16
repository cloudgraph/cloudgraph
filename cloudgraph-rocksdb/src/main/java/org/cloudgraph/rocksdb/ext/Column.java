package org.cloudgraph.rocksdb.ext;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

public class Column {
  private String name;
  private byte[] data;

  @SuppressWarnings("unused")
  private Column() {
  }

  public Column(String name, String strValue) {
    this.name = name;
    this.data = Bytes.toBytes(strValue);
  }

  public Column(String name, boolean boolValue) {
    this.name = name;
    this.data = Bytes.toBytes(boolValue);
  }

  public Column(String name, long longValue) {
    this.name = name;
    this.data = Bytes.toBytes(longValue);
  }

  public Column(String name, float floatValue) {
    this.name = name;
    this.data = Bytes.toBytes(floatValue);
  }

  public Column(String name, double doubleValue) {
    this.name = name;
    this.data = Bytes.toBytes(doubleValue);
  }

  public Column(String name, byte[] value) {
    this.name = name;
    this.data = value;
  }

  public String getName() {
    return name;
  }

  public byte[] getNameBytes() {
    return Bytes.toBytes(name);
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    if (this.data != null)
      return "Column [name=" + name + ", data=" + Bytes.toString(data) + "]";
    else
      return "Column [name=" + name + ", data=" + "NULL" + "]";

  }

}
