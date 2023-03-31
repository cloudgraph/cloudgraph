package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.BufferedMutator;

public class HBaseBufferedMutator implements BufferedMutator {
  private org.apache.hadoop.hbase.client.BufferedMutator mutator;

  @SuppressWarnings("unused")
  private HBaseBufferedMutator() {
  }

  public HBaseBufferedMutator(org.apache.hadoop.hbase.client.BufferedMutator mutator) {
    super();
    this.mutator = mutator;
  }

  public org.apache.hadoop.hbase.client.BufferedMutator getBufferedMutator() {
    return mutator;
  }
}
