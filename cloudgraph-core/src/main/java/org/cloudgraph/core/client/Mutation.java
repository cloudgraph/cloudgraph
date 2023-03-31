package org.cloudgraph.core.client;

import java.util.List;
import java.util.NavigableMap;

public interface Mutation extends Row {
  void setAttribute(String name, byte[] value);

  byte[] getAttribute(String name);

  NavigableMap<byte[], List<Cell>> getFamilyCellMap();

}
