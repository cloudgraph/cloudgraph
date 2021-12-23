package org.cloudgraph.aerospike.ext;

import org.apache.hadoop.hbase.util.Bytes;

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
