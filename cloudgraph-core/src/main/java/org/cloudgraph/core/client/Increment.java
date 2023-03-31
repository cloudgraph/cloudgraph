package org.cloudgraph.core.client;

public interface Increment extends Row, Mutation {

  void addColumn(byte[] fam, byte[] qual, long value);

}
