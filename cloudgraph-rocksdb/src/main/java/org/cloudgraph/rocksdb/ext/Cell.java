package org.cloudgraph.rocksdb.ext;

public class Cell {
  private byte[] data;

  public Cell(byte[] data) {
    super();
    this.data = data;
  }

  public byte[] getData() {
    return data;
  }

}
