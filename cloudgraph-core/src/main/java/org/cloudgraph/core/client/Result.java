package org.cloudgraph.core.client;

import java.util.List;

public interface Result extends Row {

  boolean containsColumn(byte[] family, byte[] qualifier);

  byte[] getValue(byte[] family, byte[] qualifier);

  boolean isEmpty();

  List<KeyValue> list();

  Cell[] rawCells();

  // Cell[] listCells();

  int size();

}
