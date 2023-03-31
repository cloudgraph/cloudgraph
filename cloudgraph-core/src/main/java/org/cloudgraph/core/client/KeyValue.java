package org.cloudgraph.core.client;

public interface KeyValue {

  public byte[] getRow();

  public byte[] getFamily();

  public byte[] getQualifier();

  public long getTimestamp();

  public byte[] getValue();

}
