package org.cloudgraph.rocksdb.ext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.cloudgraph.core.client.Delete;
import org.cloudgraph.core.client.Mutation;
import org.cloudgraph.core.client.Put;
import org.cloudgraph.core.client.RowMutations;

public class RocksDBRowMutations implements RowMutations {
  byte[] row;
  private List<Mutation> mutations = new ArrayList<>();

  public RocksDBRowMutations(byte[] row) {
    this.row = row;
  }

  public void add(Delete d) throws IOException {
    mutations.add(RocksDBDel.class.cast(d));
  }

  public void add(Put p) throws IOException {

    mutations.add(RocksDBPut.class.cast(p));
  }

  // public void add(Increment i) throws IOException {
  // mutations.add(HBaseIncrement.class.cast(i).get());
  // }

  public List<Mutation> getMutations() {
    return mutations;
  }

  public byte[] getRow() {
    return row;
  }

}
