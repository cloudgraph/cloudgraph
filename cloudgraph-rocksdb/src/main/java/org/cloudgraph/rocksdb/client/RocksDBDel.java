package org.cloudgraph.rocksdb.client;

import java.util.List;
import java.util.NavigableMap;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.Delete;

public class RocksDBDel extends RocksDBRowMutation implements Delete {

  public RocksDBDel(byte[] rowKey) {
    super(rowKey, MutationType.DEL);
  }

  @Override
  public void addColumns(byte[] fam, byte[] qual) {
    // FIXME: we are only able to delete an entire row at present
    // not to even distinguish particular columns
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
    throw new IllegalStateException("not implemented");
  }

}
