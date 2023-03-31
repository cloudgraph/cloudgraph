package org.cloudgraph.hbase.client;

import java.io.IOException;

import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.client.Scan;

public class HBaseScan implements Scan {
  private org.apache.hadoop.hbase.client.Scan scan;

  @SuppressWarnings("unused")
  private HBaseScan() {
  }

  public HBaseScan(org.apache.hadoop.hbase.client.Scan scan) {
    super();
    this.scan = scan;
  }

  public HBaseScan(Scan scan2) throws IOException {
    this.scan = new org.apache.hadoop.hbase.client.Scan(HBaseScan.class.cast(scan2).get());
  }

  public org.apache.hadoop.hbase.client.Scan get() {
    return scan;
  }

  @Override
  public Filter getFilter() {
    return new HBaseFilter(this.scan.getFilter());
  }

  @Override
  public byte[] getStartRow() {
    return this.scan.getStartRow();

  }

  @Override
  public byte[] getStopRow() {
    return this.scan.getStopRow();

  }

  @Override
  public void setStartRow(byte[] row) {
    this.scan.setStartRow(row);

  }

  @Override
  public void setStopRow(byte[] row) {
    this.scan.setStopRow(row);

  }

  @Override
  public void setTimeStamp(long ts) throws IOException {
    this.scan.setTimeStamp(ts);

  }

  @Override
  public void setTimeRange(long minStamp, long maxStamp) throws IOException {
    this.scan.setTimeRange(minStamp, maxStamp);

  }

  @Override
  public void setMaxVersions(int num) {
    this.scan.setMaxVersions(num);

  }

  @Override
  public void setCaching(int caching) {
    this.scan.setCaching(caching);

  }

  @Override
  public void setCacheBlocks(boolean cacheBlocks) {
    this.scan.setCacheBlocks(cacheBlocks);

  }

}
