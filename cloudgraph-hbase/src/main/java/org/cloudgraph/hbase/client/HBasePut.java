package org.cloudgraph.hbase.client;

import java.util.ArrayList;
import java.util.List;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.Put;
import org.plasma.sdo.DataType;

public class HBasePut extends HBaseMutation implements Put {
  private org.apache.hadoop.hbase.client.Put put;

  public HBasePut(org.apache.hadoop.hbase.client.Put put) {
    super(put);
    this.put = put;
  }

  public HBasePut(byte[] rowKey) {
    this(new org.apache.hadoop.hbase.client.Put(rowKey));
  }

  public org.apache.hadoop.hbase.client.Put get() {
    return this.put;
  }

  @Override
  public byte[] getRow() {
    return this.put.getRow();
  }

  @Override
  public void addColumn(byte[] family, byte[] qualifier, DataType valueDataType, byte[] value) {
    this.put.addColumn(family, qualifier, value);
  }

  @Override
  public void addColumn(byte[] fam, byte[] qual, long ts, byte[] value) {
    this.put.addColumn(fam, qual, ts, value);
  }

  @Override
  public boolean has(byte[] family, byte[] qualifier) {
    return this.put.has(family, qualifier);
  }

  @Override
  public List<Cell> get(byte[] family, byte[] qualifier) {
    List<org.apache.hadoop.hbase.Cell> list = this.put.get(family, qualifier);
    if (list != null) {
      List<Cell> resultList = new ArrayList<>(list.size());
      for (org.apache.hadoop.hbase.Cell cell : list) {
        resultList.add(new HBaseCell(cell));
      }
      return resultList;
    }
    return null;
  }

  @Override
  public int size() {
    return this.put.size();
  }

}
