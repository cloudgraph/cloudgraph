package org.cloudgraph.hbase.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.Delete;

public class HBaseDelete extends HBaseMutation implements Delete {
  private org.apache.hadoop.hbase.client.Delete delete;

  public HBaseDelete(org.apache.hadoop.hbase.client.Delete delete) {
    super(delete);
    this.delete = delete;
  }

  public HBaseDelete(byte[] rowKey) {
    this(new org.apache.hadoop.hbase.client.Delete(rowKey));
  }

  public org.apache.hadoop.hbase.client.Delete get() {
    return this.delete;
  }

  @Override
  public byte[] getRow() {
    return this.delete.getRow();
  }

  @Override
  public void setAttribute(String name, byte[] value) {
    this.delete.setAttribute(name, value);

  }

  @Override
  public byte[] getAttribute(String name) {
    return this.delete.getAttribute(name);
  }

  @Override
  public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
    NavigableMap<byte[], List<Cell>> resultMap = new TreeMap<>();
    NavigableMap<byte[], List<org.apache.hadoop.hbase.Cell>> map = this.delete.getFamilyCellMap();
    Iterator<byte[]> iter = map.keySet().iterator();
    while (iter.hasNext()) {
      byte[] key = iter.next();
      List<org.apache.hadoop.hbase.Cell> list = map.get(key);
      List<Cell> resultList = new ArrayList<>(list.size());
      resultMap.put(key, resultList);
      for (org.apache.hadoop.hbase.Cell cell : list) {
        resultList.add(new HBaseCell(cell));
      }
    }

    return resultMap;

  }

  @Override
  public void addColumns(byte[] fam, byte[] qual) {
    this.delete.addColumns(fam, qual);
  }

}
