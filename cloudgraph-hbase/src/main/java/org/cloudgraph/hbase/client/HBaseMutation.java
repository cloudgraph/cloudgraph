package org.cloudgraph.hbase.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.Mutation;

public abstract class HBaseMutation extends HBaseRow implements Mutation {
  private org.apache.hadoop.hbase.client.Mutation mutation;

  protected HBaseMutation(org.apache.hadoop.hbase.client.Mutation mutation) {
    super(mutation);
    this.mutation = mutation;
  }

  @Override
  public byte[] getRow() {
    return this.mutation.getRow();
  }

  @Override
  public void setAttribute(String name, byte[] value) {
    this.mutation.setAttribute(name, value);

  }

  @Override
  public byte[] getAttribute(String name) {
    return this.mutation.getAttribute(name);
  }

  @Override
  public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
    NavigableMap<byte[], List<Cell>> resultMap = new TreeMap<>();
    NavigableMap<byte[], List<org.apache.hadoop.hbase.Cell>> map = this.mutation.getFamilyCellMap();
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

}
