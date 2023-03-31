package org.cloudgraph.rocksdb.ext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.cloudgraph.common.Bytes;
import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.CellValues;
import org.cloudgraph.core.client.KeyValue;
import org.cloudgraph.core.client.Result;
import org.cloudgraph.rocksdb.io.CellUtil;

public class RocksDBCellValues extends CellValues {
  public RocksDBCellValues(Result row) {
    this.rowKey = row.getRow();
    this.familyMap = new HashMap<>();
    addCells(row);
  }

  public RocksDBCellValues(byte[] rowKey) {
    this.rowKey = rowKey;
    this.familyMap = new HashMap<>();
  }

  public void addAll(Result row) {
    if (!this.rowKey.equals(Bytes.toString(row.getRow())))
      throw new IllegalArgumentException("row key mismatch('" + Bytes.toString(row.getRow())
          + "') - expected '" + this.rowKey + "'");
    addCells(row);
  }

  private void addCells(Result row) {
    int fam;
    for (Cell cell : row.rawCells()) {
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
