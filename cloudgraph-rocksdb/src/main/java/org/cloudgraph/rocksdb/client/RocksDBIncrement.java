package org.cloudgraph.rocksdb.client;

import java.util.List;
import java.util.NavigableMap;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.Increment;
import org.plasma.sdo.DataType;

public class RocksDBIncrement extends RocksDBRowMutation implements Increment {

  public RocksDBIncrement(byte[] rowKey) {
    super(rowKey, MutationType.INCREMENT);
  }

  @Override
  public void setAttribute(String name, byte[] value) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public byte[] getAttribute(String name) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addColumn(byte[] fam, byte[] qual, long value) {
    super.addColumn(fam, qual, DataType.Long, org.cloudgraph.common.Bytes.toBytes(value));
  }

  // public void addColumn(byte[] fam, byte[] qual, DataType valueDataType, long
  // value) {
  // this.addColumn(fam, qual, valueDataType, Bytes.toBytes(value));
  // }

}
