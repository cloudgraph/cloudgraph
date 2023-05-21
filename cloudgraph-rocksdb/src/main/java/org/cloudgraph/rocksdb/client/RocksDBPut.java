package org.cloudgraph.rocksdb.client;

import java.util.List;
import java.util.NavigableMap;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.Put;
import org.plasma.sdo.DataType;

public class RocksDBPut extends RocksDBRowMutation implements Put {

  public RocksDBPut(byte[] rowKey) {
    super(rowKey, MutationType.PUT);
  }

  @Override
  public void addColumn(byte[] fam, byte[] qual, DataType valueDataType, byte[] value) {
    super.addColumn(fam, qual, valueDataType, value);
  }

  @Override
  public void addColumn(byte[] fam, byte[] qual, long ts, byte[] value) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public void setAttribute(String name, byte[] value) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public byte[] getAttribute(String name) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
    throw new RuntimeException("not implemented");
  }

}
