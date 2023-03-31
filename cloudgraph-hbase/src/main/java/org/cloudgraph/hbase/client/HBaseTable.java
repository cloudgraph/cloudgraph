package org.cloudgraph.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.cloudgraph.core.client.CompareOper;
import org.cloudgraph.core.client.Get;
import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.client.ResultScanner;
import org.cloudgraph.core.client.Row;
import org.cloudgraph.core.client.RowMutations;
import org.cloudgraph.core.client.Scan;
import org.cloudgraph.core.client.Table;

public class HBaseTable implements Table {
  private org.apache.hadoop.hbase.client.Table table;

  @SuppressWarnings("unused")
  private HBaseTable() {
  }

  public HBaseTable(org.apache.hadoop.hbase.client.Table table) {
    super();
    this.table = table;
  }

  public org.apache.hadoop.hbase.client.Table get() {
    return table;
  }

  @Override
  public Result get(Get get) throws IOException {
    org.cloudgraph.hbase.client.HBaseGet hbaseGet = (org.cloudgraph.hbase.client.HBaseGet) get;
    return new HBaseResult(this.table.get(hbaseGet.get()));
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    List<org.apache.hadoop.hbase.client.Get> hbaseGets = new ArrayList<>(gets.size());
    for (Get get : gets) {
      hbaseGets.add(((HBaseGet) get).get());
    }
    org.apache.hadoop.hbase.client.Result[] hbaseResults = this.table.get(hbaseGets);
    Result[] results = new Result[hbaseResults.length];
    for (int i = 0; i < hbaseResults.length; i++) {
      results[i] = new HBaseResult(hbaseResults[i]);
    }

    return results;
  }

  @Override
  public void batch(List<Row> rows, Object[] results) throws InterruptedException, IOException {

    List<org.apache.hadoop.hbase.client.Row> hbaseRows = new ArrayList<>(rows.size());
    for (Row row : rows) {
      hbaseRows.add(((HBaseRow) row).get());
    }

    this.table.batch(hbaseRows, results);
  }

  @Override
  public String getName() {
    return this.table.getName().toString();
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] checkFamBytes, byte[] checkQualBytes,
      CompareOper oper, byte[] checkValueBytes, RowMutations checkedMutations) throws IOException {

    HBaseRowMutations hbaseCheckedMutations = (HBaseRowMutations) checkedMutations;

    return this.table.checkAndMutate(row, checkFamBytes, checkQualBytes, asHBaseCompareOp(oper),
        checkValueBytes, hbaseCheckedMutations.get());
  }

  private CompareOp asHBaseCompareOp(CompareOper oper) {
    switch (oper) {
    case EQUAL:
      return CompareOp.EQUAL;
    case GREATER:
      return CompareOp.GREATER;
    case GREATER_OR_EQUAL:
      return CompareOp.GREATER_OR_EQUAL;
    case LESS:
      return CompareOp.LESS;
    case LESS_OR_EQUAL:
      return CompareOp.LESS_OR_EQUAL;
    case NOT_EQUAL:
      return CompareOp.NOT_EQUAL;
    case NO_OP:
      return CompareOp.NO_OP;
    default:
      throw new IllegalArgumentException("unknown oper, " + oper);
    }
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return new HBaseResultScanner(this.table.getScanner(HBaseScan.class.cast(scan).get()));
  }

  @Override
  public Object getConfiguration() {
    return this.table.getConfiguration();
  }

}
