package org.cloudgraph.rocksdb.ext;

import java.io.IOException;

import org.cloudgraph.core.client.Filter;

public class RocksDBFilter implements Filter {

  private org.cloudgraph.rocksdb.filter.Filter filter;

  public RocksDBFilter(org.cloudgraph.rocksdb.filter.Filter filter) {
    this.filter = filter;
  }

  public org.cloudgraph.rocksdb.filter.Filter get() {
    return filter;
  }

  @Override
  public String printFilterTree() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
