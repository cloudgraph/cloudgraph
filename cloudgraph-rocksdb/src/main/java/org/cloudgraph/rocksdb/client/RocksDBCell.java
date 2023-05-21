package org.cloudgraph.rocksdb.client;

import org.cloudgraph.core.client.Cell;

public class RocksDBCell implements Cell {
  private byte[] fam;
  private byte[] qual;
  private byte[] value;

  @SuppressWarnings("unused")
  private RocksDBCell() {
  }

  public RocksDBCell(byte[] fam, byte[] qual, byte[] value) {
    super();
    this.fam = fam;
    this.qual = qual;
    this.value = value;
  }

  public byte[] getData() {
    return value;
  }

  @Override
  public byte[] getFamilyArray() {
    return this.fam;
  }

  @Override
  public int getFamilyOffset() {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public int getFamilyLength() {
    return this.fam.length;
  }

  @Override
  public byte[] getQualifierArray() {
    return this.qual;
  }

  @Override
  public int getQualifierOffset() {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public int getQualifierLength() {
    return this.qual.length;
  }

  @Override
  public byte[] getValueArray() {
    return this.value;
  }

  @Override
  public int getValueOffset() {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public int getValueLength() {
    return this.value.length;
  }

}
