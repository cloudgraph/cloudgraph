package org.cloudgraph.rocksdb.client;

import java.io.IOException;

import org.cloudgraph.core.client.Scan;
import org.cloudgraph.core.scan.ScanLiterals;

public class RocksDBScan implements Scan {
  private byte[] startRow;
  private byte[] stopRow;
  private ScanLiterals scanLiterals;
  private org.cloudgraph.core.client.Filter columnFilter;

  @SuppressWarnings("unused")
  private RocksDBScan() {
  }

  public RocksDBScan(org.cloudgraph.core.client.Filter columnFilter) {
    this.columnFilter = columnFilter;
  }

  public RocksDBScan(byte[] startRow, byte[] stopRow, org.cloudgraph.core.client.Filter columnFilter) {
    this(columnFilter);
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.columnFilter = columnFilter;
  }

  public RocksDBScan(ScanLiterals scanLiterals, org.cloudgraph.core.client.Filter columnFilter) {
    this(columnFilter);
    this.scanLiterals = scanLiterals;
    if (scanLiterals == null)
      throw new IllegalArgumentException("expected scanLiterals");
  }

  public boolean hasScanLiterals() {
    return scanLiterals != null;
  }

  public ScanLiterals getScanLiterals() {
    return scanLiterals;
  }

  public org.cloudgraph.core.client.Filter getColumnFilter() {
    return columnFilter;
  }

  public void setColumnFilter(org.cloudgraph.core.client.Filter rootFilter) {
    this.columnFilter = rootFilter;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public void setStartRow(byte[] startRow) {
    this.startRow = startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  public void setStopRow(byte[] stopRow) {
    this.stopRow = stopRow;
  }

  @Override
  public org.cloudgraph.core.client.Filter getFilter() {
    return columnFilter;
  }

  @Override
  public void setTimeStamp(long parseLong) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setTimeRange(long parseLong, long parseLong2) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setMaxVersions(int parseInt) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setCaching(int parseInt) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setCacheBlocks(boolean b) {
    // TODO Auto-generated method stub

  }

}
