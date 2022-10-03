package org.cloudgraph.rocksdb.ext;

public class Put extends RowMutation {

  public Put(byte[] rowKey) {
    super(rowKey, MutationType.PUT);
  }

}
