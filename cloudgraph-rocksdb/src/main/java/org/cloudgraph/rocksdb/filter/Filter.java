package org.cloudgraph.rocksdb.filter;

import java.util.Set;

public abstract class Filter {

  public abstract Set<String> getColumnKeys();

  public abstract String[] getColumnNames();
}
