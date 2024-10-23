package org.cloudgraph.hbase.client;

 import org.apache.hadoop.hbase.HRegionInfo;
import org.cloudgraph.core.client.RegionLocation;

public class HBaseRegionLocation implements RegionLocation {
  private org.apache.hadoop.hbase.HRegionLocation regionLocation;
  private HRegionInfo regionInfo;
  
  @SuppressWarnings("unused")
  private HBaseRegionLocation() {
  }

  public HBaseRegionLocation(org.apache.hadoop.hbase.HRegionLocation regionLocation) {
    super();
    this.regionLocation = regionLocation;
    this.regionInfo = this.regionLocation.getRegionInfo();
  }

  @Override
  public String getHostname() {
    return regionLocation.getHostname();
  }

  @Override
  public int getPort() {
    return regionLocation.getPort();
  }

  @Override
  public long getSeqNum() {
    return regionLocation.getSeqNum();
  }

  @Override
  public String getHostnamePort() {
    return regionLocation.getHostnamePort();
  }

  @Override
  public long getRegionId() {
    return regionInfo.getRegionId();
  }

  @Override
  public String getRegionNameAsString() {
    return regionInfo.getRegionNameAsString();
  }

  @Override
  public byte[] getStartKey() {
    return regionInfo.getStartKey();
  }

  @Override
  public byte[] getEndKey() {
    return regionInfo.getEndKey();
  }

 
}
