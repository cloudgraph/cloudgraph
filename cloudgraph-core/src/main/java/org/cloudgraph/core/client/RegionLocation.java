package org.cloudgraph.core.client;

public interface RegionLocation {

  String getHostname();

  int getPort();

  long getSeqNum();

  String getHostnamePort();

  long getRegionId();

  String getRegionNameAsString();

  byte[] getStartKey();

  byte[] getEndKey();

}
