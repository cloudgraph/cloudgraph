package org.cloudgraph.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.cloudgraph.core.client.CellUtil;
import org.cloudgraph.core.client.CellValues;
import org.cloudgraph.core.client.ClientFactory;
import org.cloudgraph.core.client.Delete;
import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.client.FilterList;
import org.cloudgraph.core.client.FilterList.Operator;
import org.cloudgraph.core.client.Get;
import org.cloudgraph.core.client.Increment;
import org.cloudgraph.core.client.Put;
import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.client.RowMutations;
import org.cloudgraph.core.client.Scan;
import org.cloudgraph.core.client.TableName;
import org.cloudgraph.core.scan.CompleteRowKey;
import org.cloudgraph.core.scan.FuzzyRowKey;
import org.cloudgraph.core.scan.PartialRowKey;
import org.cloudgraph.hbase.io.HBaseCellValues;

public class HBaseClientFactory implements ClientFactory {

  @Override
  public Put createPut(byte[] rowKey) {
    return new HBasePut(rowKey);
  }

  @Override
  public Delete createDelete(byte[] rowKey) {
    return new HBaseDelete(rowKey);
  }

  @Override
  public Increment createIncrement(byte[] rowKey) {
    return new HBaseIncrement(rowKey);
  }

  @Override
  public Get createGet(byte[] rowKey) {
    return new HBaseGet(rowKey);
  }

  @Override
  public CellValues createCellValues(byte[] rowKey) {
    return new HBaseCellValues(rowKey);
  }

  @Override
  public CellUtil getCellUtil() {
    return null;
  }

  @Override
  public RowMutations createRowMutations(byte[] row) {
    return new HBaseRowMutations(row);
  }

  @Override
  public CellValues createCellValues(Result result) {
    return new HBaseCellValues(result);
  }

  @Override
  public Scan createPartialRowKeyScan(PartialRowKey partialRowKey, Filter columnFilter) {
    org.apache.hadoop.hbase.filter.FilterList rootFilter = new org.apache.hadoop.hbase.filter.FilterList(
        org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(HBaseFilter.class.cast(columnFilter).get());
    org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
    scan.setFilter(rootFilter);
    scan.setStartRow(partialRowKey.getStartKey()); // inclusive
    scan.setStopRow(partialRowKey.getStopKey()); // exclusive
    return new HBaseScan(scan);
  }

  @Override
  public Get createGet(CompleteRowKey rowKey, Filter columnFilter) {
    // FIXME: root filter ignored
    org.apache.hadoop.hbase.filter.FilterList rootFilter = new org.apache.hadoop.hbase.filter.FilterList(
        org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(HBaseFilter.class.cast(columnFilter).get());
    org.apache.hadoop.hbase.client.Get get = new org.apache.hadoop.hbase.client.Get(rowKey.getKey());
    get.setFilter(HBaseFilter.class.cast(columnFilter).get());
    return new HBaseGet(get);
  }

  @Override
  public Scan createScan(Filter fuzzyRowFilter, Filter columnFilter) {
    org.apache.hadoop.hbase.filter.FilterList rootFilter = new org.apache.hadoop.hbase.filter.FilterList(
        org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(HBaseFilter.class.cast(fuzzyRowFilter).get());
    rootFilter.addFilter(HBaseFilter.class.cast(columnFilter).get());
    org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
    scan.setFilter(rootFilter);
    return new HBaseScan(scan);
  }

  @Override
  public Scan createScan(Float sample, Filter columnFilter) {
    org.apache.hadoop.hbase.filter.FilterList rootFilter = new org.apache.hadoop.hbase.filter.FilterList(
        org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(HBaseFilter.class.cast(columnFilter).get());
    org.apache.hadoop.hbase.filter.RandomRowFilter rowFilter = new org.apache.hadoop.hbase.filter.RandomRowFilter(
        sample);
    rootFilter.addFilter(rowFilter);
    rootFilter.addFilter(HBaseFilter.class.cast(columnFilter).get());
    org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
    scan.setFilter(rootFilter);
    return new HBaseScan(scan);
  }

  @Override
  public Scan createScan(Filter columnFilter) {
    org.apache.hadoop.hbase.filter.FilterList rootFilter = new org.apache.hadoop.hbase.filter.FilterList(
        org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(HBaseFilter.class.cast(columnFilter).get());
    org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
    scan.setFilter(rootFilter);
    return new HBaseScan(scan);
  }

  @Override
  public Scan createScan(FuzzyRowKey fuzzyScan, Filter columnFilter) {
    org.apache.hadoop.hbase.filter.FilterList rootFilter = new org.apache.hadoop.hbase.filter.FilterList(
        org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(HBaseFilter.class.cast(columnFilter).get());
    Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>(fuzzyScan.getFuzzyKeyBytes(),
        fuzzyScan.getFuzzyInfoBytes());
    List<Pair<byte[], byte[]>> list = new ArrayList<Pair<byte[], byte[]>>();
    list.add(pair);
    rootFilter.addFilter(new FuzzyRowFilter(list));
    org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
    scan.setFilter(rootFilter);
    return new HBaseScan(scan);
  }

  @Override
  public Scan createScan(Scan scan) throws IOException {
    return new HBaseScan(scan);
  }

  @Override
  public TableName createTableName(String tableNamespace, String tableName) {

    return HBaseTableName.valueOf(tableNamespace, tableName);
  }

}
