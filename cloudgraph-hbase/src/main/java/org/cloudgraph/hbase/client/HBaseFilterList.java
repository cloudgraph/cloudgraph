package org.cloudgraph.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.client.FilterList;

public class HBaseFilterList implements FilterList {

  private Operator operator;
  private List<Filter> filters = Collections.emptyList();

  public HBaseFilterList(Operator oper) {
    this.operator = oper;
  }

  @Override
  public Operator getOperator() {
    return operator;
  }

  @Override
  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  @Override
  public List<Filter> getFilters() {
    return filters;
  }

  @Override
  public void addFilter(Filter filter) {
    if (filters.size() == 0)
      filters = new ArrayList<>(); // overwrite immutable list
    this.filters.add(filter);

  }

  @Override
  public void addFilter(List<Filter> filters) {
    if (filters.size() == 0)
      filters = new ArrayList<>(); // overwrite immutable list
    this.filters.addAll(filters);
  }

  @Override
  public String printFilterTree() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
