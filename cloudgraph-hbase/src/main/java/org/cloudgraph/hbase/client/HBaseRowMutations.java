package org.cloudgraph.hbase.client;

import java.io.IOException;
import java.util.List;

import org.cloudgraph.core.client.Delete;
import org.cloudgraph.core.client.Put;
import org.cloudgraph.core.client.RowMutations;

public class HBaseRowMutations implements RowMutations {
  private org.apache.hadoop.hbase.client.RowMutations mutations;

  public HBaseRowMutations(org.apache.hadoop.hbase.client.RowMutations mutations) {
    super();
    this.mutations = mutations;
  }

  public HBaseRowMutations(byte[] row) {
    this(new org.apache.hadoop.hbase.client.RowMutations(row));
  }

  public org.apache.hadoop.hbase.client.RowMutations get() {
    return mutations;
  }

  public void add(Delete d) throws IOException {
    mutations.add(HBaseDelete.class.cast(d).get());
  }

  public void add(Put p) throws IOException {

    mutations.add(HBasePut.class.cast(p).get());
  }

  // public void add(Increment i) throws IOException {
  // mutations.add(HBaseIncrement.class.cast(i).get());
  // }

  public List<org.apache.hadoop.hbase.client.Mutation> getMutations() {
    return mutations.getMutations();
  }

  public byte[] getRow() {
    return mutations.getRow();
  }

}
