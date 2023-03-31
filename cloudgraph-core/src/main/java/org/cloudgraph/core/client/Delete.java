package org.cloudgraph.core.client;

public interface Delete extends Row, Mutation {

  void addColumns(byte[] fam, byte[] qual);

}
