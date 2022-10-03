package org.cloudgraph.rocksdb.ext;

import org.cloudgraph.common.Bytes;
import org.cloudgraph.rocksdb.ext.RowMutation.MutationType;
import org.plasma.sdo.DataType;

public class Increment extends RowMutation {

  public Increment(byte[] rowKey) {
    super(rowKey, MutationType.INCREMENT);
  }

  // public void addColumn(byte[] fam, byte[] qual, DataType valueDataType, long
  // value) {
  // this.addColumn(fam, qual, valueDataType, Bytes.toBytes(value));
  // }

}
