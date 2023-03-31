package org.cloudgraph.hbase.filter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.filter.RowFilterFactory;
import org.cloudgraph.core.scan.FuzzyRowKey;
import org.cloudgraph.hbase.client.HBaseFilter;

public class HBaseRowFilterFactory implements RowFilterFactory {

  @Override
  public Filter createFuzzyRowFilter(FuzzyRowKey fuzzyScan) {
    Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>(fuzzyScan.getFuzzyKeyBytes(),
        fuzzyScan.getFuzzyInfoBytes());
    List<Pair<byte[], byte[]>> list = new ArrayList<Pair<byte[], byte[]>>();
    list.add(pair);
    return new HBaseFilter(new FuzzyRowFilter(list));
  }

  @Override
  public Filter createRandomRowFilter(Float sample, Filter columnFilter) {
    org.apache.hadoop.hbase.filter.FilterList rootFilter = new org.apache.hadoop.hbase.filter.FilterList(
        org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(HBaseFilter.class.cast(columnFilter).get());
    org.apache.hadoop.hbase.filter.RandomRowFilter rowFilter = new org.apache.hadoop.hbase.filter.RandomRowFilter(
        sample);
    rootFilter.addFilter(rowFilter);
    return new HBaseFilter(rootFilter);
  }

}
