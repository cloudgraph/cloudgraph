package org.cloudgraph.hbase.io;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.core.client.CellValues;
import org.cloudgraph.hbase.client.HBaseResult;

public class HBaseCellValues extends CellValues {
  public HBaseCellValues(Result row) {
    this.rowKey = row.getRow();
    this.familyMap = new HashMap<>();
    addCells(row);
  }

  public HBaseCellValues(byte[] rowKey) {
    this.rowKey = rowKey;
    this.familyMap = new HashMap<>();
  }

  public HBaseCellValues(org.cloudgraph.core.client.Result result) {
    this(HBaseResult.class.cast(result).get());
  }

  public void addAll(Result row) {
    if (!this.rowKey.equals(Bytes.toString(row.getRow())))
      throw new IllegalArgumentException("row key mismatch('" + Bytes.toString(row.getRow())
          + "') - expected '" + this.rowKey + "'");
    addCells(row);
  }

  private void addCells(Result row) {
    int fam;
    for (Cell cell : row.listCells()) {
      fam = Arrays.hashCode(CellUtil.cloneFamily(cell));
      Map<Integer, byte[]> map = this.familyMap.get(fam);
      if (map == null) {
        map = new HashMap<>();
        this.familyMap.put(fam, map);
      }
      byte[] qual = CellUtil.cloneQualifier(cell);
      map.put(Arrays.hashCode(qual), CellUtil.cloneValue(cell));
    }
  }

  public void addColumn(KeyValue keyValue) {
    int fam = Arrays.hashCode(CellUtil.cloneFamily(keyValue));
    Map<Integer, byte[]> map = this.familyMap.get(fam);
    if (map == null) {
      map = new HashMap<>();
      this.familyMap.put(fam, map);
    }
    byte[] qual = CellUtil.cloneQualifier(keyValue);
    map.put(Arrays.hashCode(qual), CellUtil.cloneValue(keyValue));
  }

}
