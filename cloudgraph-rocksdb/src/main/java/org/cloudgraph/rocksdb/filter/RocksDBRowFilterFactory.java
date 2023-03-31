package org.cloudgraph.rocksdb.filter;

import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.filter.RowFilterFactory;
import org.cloudgraph.core.scan.FuzzyRowKey;

public class RocksDBRowFilterFactory implements RowFilterFactory {

  @Override
  public Filter createFuzzyRowFilter(FuzzyRowKey fuzzyScan) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public Filter createRandomRowFilter(Float sample, Filter columnFilter) {
    throw new IllegalStateException("not implemented");
  }

}
