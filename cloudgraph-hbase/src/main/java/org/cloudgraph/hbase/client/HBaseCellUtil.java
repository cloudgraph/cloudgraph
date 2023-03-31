package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.CellUtil;

public class HBaseCellUtil implements CellUtil {

  @Override
  public byte[] cloneValue(Cell cell) {
    return org.apache.hadoop.hbase.CellUtil.cloneValue(HBaseCell.class.cast(cell).get());
  }

  @Override
  public byte[] cloneQualifier(Cell cell) {
    return org.apache.hadoop.hbase.CellUtil.cloneQualifier(HBaseCell.class.cast(cell).get());
  }

  @Override
  public byte[] cloneFamily(Cell cell) {
    return org.apache.hadoop.hbase.CellUtil.cloneFamily(HBaseCell.class.cast(cell).get());
  }

}
