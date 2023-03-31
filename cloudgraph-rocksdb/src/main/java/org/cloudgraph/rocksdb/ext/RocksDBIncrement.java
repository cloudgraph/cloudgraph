package org.cloudgraph.rocksdb.ext;

import java.util.List;
import java.util.NavigableMap;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.Increment;

public class RocksDBIncrement extends RocksDBRowMutation implements Increment {

  public RocksDBIncrement(byte[] rowKey) {
    super(rowKey, MutationType.INCREMENT);
  }

  @Override
  public void setAttribute(String name, byte[] value) {
    // TODO Auto-generated method stub

  }

  @Override
  public byte[] getAttribute(String name) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addColumn(byte[] fam, byte[] qual, long value) {
    // TODO Auto-generated method stub

  }

  // public void addColumn(byte[] fam, byte[] qual, DataType valueDataType, long
  // value) {
  // this.addColumn(fam, qual, valueDataType, Bytes.toBytes(value));
  // }

}
