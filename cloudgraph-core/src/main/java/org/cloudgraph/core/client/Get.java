package org.cloudgraph.core.client;

import java.util.Map;
import java.util.NavigableSet;

public interface Get extends Row {

  void addColumn(byte[] family, byte[] qualifier);

  void setFilter(Filter filter);

  Filter getFilter();

  Map<byte[], NavigableSet<byte[]>> getFamilyMap();

}
