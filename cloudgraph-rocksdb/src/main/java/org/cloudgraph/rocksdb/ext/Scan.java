package org.cloudgraph.rocksdb.ext;

import org.cloudgraph.rocksdb.filter.Filter;
import org.cloudgraph.rocksdb.scan.ScanLiterals;

public class Scan {
  private byte[] startRow;
  private byte[] stopRow;
  private ScanLiterals scanLiterals;
  private Filter columnFilter;

  @SuppressWarnings("unused")
  private Scan() {
  }

  public Scan(Filter columnFilter) {
    this.columnFilter = columnFilter;
  }

  public Scan(ScanLiterals scanLiterals, Filter columnFilter) {
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

  public Filter getColumnFilter() {
    return columnFilter;
  }

  public void setCOlumnFilter(Filter rootFilter) {
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

}
