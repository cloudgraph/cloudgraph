package org.cloudgraph.core.client;

import java.util.List;

public interface FilterList extends Filter {
  public enum Operator {
    AND, OR;
  }

  Operator getOperator();

  void setOperator(Operator operator);

  List<Filter> getFilters();

  void addFilter(Filter filter);

  void addFilter(List<Filter> filters);
}
