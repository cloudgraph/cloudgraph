package org.cloudgraph.rocksdb.ext;

import java.util.List;
import java.util.NavigableMap;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.Delete;

public class RocksDBDel extends RocksDBRowMutation implements Delete {

  public RocksDBDel(byte[] rowKey) {
    super(rowKey, MutationType.DEL);
  }

  @Override
  public byte[] getRow() {
    // TODO Auto-generated method stub
    return null;
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
  public void addColumns(byte[] fam, byte[] qual) {
    // TODO Auto-generated method stub

  }

}
