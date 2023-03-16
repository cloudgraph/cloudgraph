package org.cloudgraph.rocksdb.io;

import org.apache.hadoop.hbase.util.Bytes;

public class KeyValue {
  private byte[] row;
  private byte[] family;
  private byte[] qualifier;
  private byte[] value;

  @SuppressWarnings("unused")
  private KeyValue() {
  }

  public KeyValue(byte[] row, byte[] family, byte[] qualifier, byte[] value) {
    super();
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
    this.value = value;
    if (this.row == null || this.row.length == 0)
      throw new IllegalArgumentException("expected valid row");
    if (this.family == null || this.family.length == 0)
      throw new IllegalArgumentException("expected valid family");
    if (this.qualifier == null || this.qualifier.length == 0)
      throw new IllegalArgumentException("expected valid qualifier");
    if (this.value == null)
      throw new IllegalArgumentException("expected valid value");
    // String qualStr = Bytes.toString(qualifier);
    // if (this.value != null && this.value.length == 8) {
    // int foo = 0;
    // foo++;
    // }

  }

  public byte[] getRow() {
    return row;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public byte[] getValue() {
    return value;
  }

}
