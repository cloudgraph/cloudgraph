package org.cloudgraph.hbase.client;

import java.io.IOException;

import org.cloudgraph.core.client.Filter;
import org.cloudgraph.hbase.util.FilterUtil;

public class HBaseFilter implements Filter {

  org.apache.hadoop.hbase.filter.Filter filter;

  @SuppressWarnings("unused")
  private HBaseFilter() {
  }

  public HBaseFilter(org.apache.hadoop.hbase.filter.Filter filter) {
    super();
    this.filter = filter;
  }

  public org.apache.hadoop.hbase.filter.Filter get() {
    return filter;
  }

  @Override
  public String printFilterTree() throws IOException {
    return FilterUtil.printFilterTree(this.filter);
  }

}
