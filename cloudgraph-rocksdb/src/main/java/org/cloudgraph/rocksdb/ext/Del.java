package org.cloudgraph.rocksdb.ext;

public class Del extends RowMutation {

  public Del(byte[] rowKey) {
    super(rowKey, MutationType.DEL);
  }

}
