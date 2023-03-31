package org.cloudgraph.core.client;

public interface Cell {

  byte[] getFamilyArray();

  int getFamilyOffset();

  int getFamilyLength();

  byte[] getQualifierArray();

  int getQualifierOffset();

  int getQualifierLength();

  byte[] getValueArray();

  int getValueOffset();

  int getValueLength();

}
