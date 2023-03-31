package org.cloudgraph.core.client;

import java.util.List;

import org.plasma.sdo.DataType;

public interface Put extends Row, Mutation {

  void addColumn(byte[] fam, byte[] qual, DataType valueDataType, byte[] value);

  void addColumn(byte[] fam, byte[] qual, long ts, byte[] value);

  boolean has(byte[] family, byte[] qualifier);

  List<Cell> get(byte[] family, byte[] qualifier);

  int size();

}
