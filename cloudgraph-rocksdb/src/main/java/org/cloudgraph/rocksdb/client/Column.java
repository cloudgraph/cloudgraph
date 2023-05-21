package org.cloudgraph.rocksdb.client;

import org.cloudgraph.common.Bytes;

public class Column {
  private String name;
  private byte[] data;
  private String dataString;

  @SuppressWarnings("unused")
  private Column() {
  }

  public Column(String name, String strValue) {
    this.name = name;
    this.data = Bytes.toBytes(strValue);
    this.dataString = strValue;
  }

  public Column(String name, boolean boolValue) {
    this.name = name;
    this.data = Bytes.toBytes(boolValue);
    this.dataString = String.valueOf(boolValue);
  }

  public Column(String name, long longValue) {
    this.name = name;
    this.data = Bytes.toBytes(longValue);
    this.dataString = String.valueOf(longValue);
  }

  public Column(String name, float floatValue) {
    this.name = name;
    this.data = Bytes.toBytes(floatValue);
    this.dataString = String.valueOf(floatValue);
  }

  public Column(String name, double doubleValue) {
    this.name = name;
    this.data = Bytes.toBytes(doubleValue);
    this.dataString = String.valueOf(doubleValue);
  }

  public Column(String name, byte[] value) {
    this.name = name;
    this.data = value;
    this.dataString = Bytes.toString(value);
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

  public String getDataDebugString() {
    return this.dataString;
  }

  @Override
  public String toString() {
    if (this.data != null)
      return "Column [name=" + name + ", data=" + this.dataString + "]";
    else
      return "Column [name=" + name + ", data=" + "NULL" + "]";

  }

}
