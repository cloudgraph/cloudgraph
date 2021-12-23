package org.cloudgraph.aerospike.ext;

import org.cloudgraph.common.Bytes;
import org.plasma.sdo.DataType;

public class Increment extends Put {

  public Increment(byte[] rowKey) {
    super(rowKey);
  }

  public void addColumn(byte[] fam, byte[] qual, DataType valueDataType, long value) {
    this.addColumn(fam, qual, valueDataType, Bytes.toBytes(value));
  }

}
