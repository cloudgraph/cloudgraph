package org.cloudgraph.aerospike;

public interface AerospikeConstants {
  /** the max number of characters in an aerospike bin (~column) */
  public int BIN_MAX = 14;
  /**
   * a number of side row sequences to fetch by default given no Aerospike
   * column/bin prefix filter
   */
  public int FETCH_HACK = 9;

}
