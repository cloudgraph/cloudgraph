package org.cloudgraph.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;
import org.cloudgraph.core.client.RegionLocation;
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

  @Override
  public List<RegionLocation> getAllRegionLocations() throws IOException {
    List<HRegionLocation> locs = this.regionLocator.getAllRegionLocations();
    
    List<RegionLocation> result = new ArrayList<>(locs.size());
    for (HRegionLocation loc : locs) {
       result.add(new HBaseRegionLocation(loc));
    }

    return result;
  }

}
