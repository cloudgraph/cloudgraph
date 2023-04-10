package org.cloudgraph.hbase.client;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionLoadStats;
import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.KeyValue;
import org.cloudgraph.core.client.Result;

public class HBaseResult implements Result {

  private org.apache.hadoop.hbase.client.Result result;

  @SuppressWarnings("unused")
  private HBaseResult() {
  }

  public HBaseResult(org.apache.hadoop.hbase.client.Result result) {
    this.result = result;
  }

  public org.apache.hadoop.hbase.client.Result get() {
    return result;
  }

  @Override
  public List<KeyValue> list() {
    List<org.apache.hadoop.hbase.KeyValue> hbaseResults = this.result.list();
    List<KeyValue> results = new ArrayList<KeyValue>(hbaseResults.size());
    for (org.apache.hadoop.hbase.KeyValue hkv : hbaseResults) {
      results.add(new HBaseKeyValue(hkv));
    }
    return results;
  }

  @Override
  public Cell[] rawCells() {
    org.apache.hadoop.hbase.Cell[] hbaseResults = this.result.rawCells();
    if (hbaseResults != null) {
      Cell[] results = new Cell[hbaseResults.length];
      int i = 0;
      for (org.apache.hadoop.hbase.Cell hcell : hbaseResults) {
        results[i] = new HBaseCell(hcell);
        i++;
      }
      return results;
    } else {
      return null;
    }
  }

  public boolean advance() {
    return result.advance();
  }

  public CellScanner cellScanner() {
    return result.cellScanner();
  }

  public boolean containsColumn(byte[] family, byte[] qualifier) {
    return result.containsColumn(family, qualifier);
  }

  public boolean containsColumn(byte[] family, int foffset, int flength, byte[] qualifier,
      int qoffset, int qlength) {
    return result.containsColumn(family, foffset, flength, qualifier, qoffset, qlength);
  }

  public boolean containsEmptyColumn(byte[] family, byte[] qualifier) {
    return result.containsEmptyColumn(family, qualifier);
  }

  public boolean containsEmptyColumn(byte[] family, int foffset, int flength, byte[] qualifier,
      int qoffset, int qlength) {
    return result.containsEmptyColumn(family, foffset, flength, qualifier, qoffset, qlength);
  }

  public boolean containsNonEmptyColumn(byte[] family, byte[] qualifier) {
    return result.containsNonEmptyColumn(family, qualifier);
  }

  public boolean containsNonEmptyColumn(byte[] family, int foffset, int flength, byte[] qualifier,
      int qoffset, int qlength) {
    return result.containsNonEmptyColumn(family, foffset, flength, qualifier, qoffset, qlength);
  }

  public void copyFrom(org.apache.hadoop.hbase.client.Result other) {
    result.copyFrom(other);
  }

  public org.apache.hadoop.hbase.Cell current() {
    return result.current();
  }

  public boolean equals(Object obj) {
    return result.equals(obj);
  }

  public org.apache.hadoop.hbase.Cell getColumnLatestCell(byte[] family, byte[] qualifier) {
    return result.getColumnLatestCell(family, qualifier);
  }

  public org.apache.hadoop.hbase.Cell getColumnLatestCell(byte[] family, int foffset, int flength,
      byte[] qualifier, int qoffset, int qlength) {
    return result.getColumnLatestCell(family, foffset, flength, qualifier, qoffset, qlength);
  }

  public Boolean getExists() {
    return result.getExists();
  }

  public NavigableMap<byte[], byte[]> getFamilyMap(byte[] arg0) {
    return result.getFamilyMap(arg0);
  }

  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
    return result.getMap();
  }

  public NavigableMap<byte[], NavigableMap<byte[], byte[]>> getNoVersionMap() {
    return result.getNoVersionMap();
  }

  public byte[] getRow() {
    return result.getRow();
  }

  public RegionLoadStats getStats() {
    return result.getStats();
  }

  public byte[] getValue(byte[] family, byte[] qualifier) {
    return result.getValue(family, qualifier);
  }

  public ByteBuffer getValueAsByteBuffer(byte[] family, byte[] qualifier) {
    return result.getValueAsByteBuffer(family, qualifier);
  }

  public ByteBuffer getValueAsByteBuffer(byte[] family, int foffset, int flength, byte[] qualifier,
      int qoffset, int qlength) {
    return result.getValueAsByteBuffer(family, foffset, flength, qualifier, qoffset, qlength);
  }

  public int hashCode() {
    return result.hashCode();
  }

  public boolean isEmpty() {
    return result.isEmpty();
  }

  public boolean isPartial() {
    return result.isPartial();
  }

  public boolean isStale() {
    return result.isStale();
  }

  public List<org.apache.hadoop.hbase.Cell> listCells() {
    return result.listCells();
  }

  public boolean loadValue(byte[] family, byte[] qualifier, ByteBuffer dst)
      throws BufferOverflowException {
    return result.loadValue(family, qualifier, dst);
  }

  public boolean loadValue(byte[] family, int foffset, int flength, byte[] qualifier, int qoffset,
      int qlength, ByteBuffer dst) throws BufferOverflowException {
    return result.loadValue(family, foffset, flength, qualifier, qoffset, qlength, dst);
  }

  public void setExists(Boolean exists) {
    result.setExists(exists);
  }

  public void setStatistics(RegionLoadStats loadStats) {
    result.setStatistics(loadStats);
  }

  public int size() {
    return result.size();
  }

  public String toString() {
    return result.toString();
  }

  public byte[] value() {
    return result.value();
  }

}
