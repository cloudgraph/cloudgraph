package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.RegionLocator;

public class HBaseRegionLocator implements RegionLocator {
  private org.apache.hadoop.hbase.client.RegionLocator regionLocator;

  @SuppressWarnings("unused")
  private HBaseRegionLocator() {
  }

  public HBaseRegionLocator(org.apache.hadoop.hbase.client.RegionLocator regionLocator) {
    super();
    this.regionLocator = regionLocator;
  }

  public org.apache.hadoop.hbase.client.RegionLocator getRegionLocator() {
    return regionLocator;
  }

}
