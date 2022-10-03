package org.cloudgraph.rocksdb.filter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FilterList extends Filter {
  public enum Operator {
    AND, OR;
  }

  private static List<Filter> EMPTY_LIST = new ArrayList<>(0);
  private static String[] EMPTY_ARRAY = new String[0];
  private static Set<String> EMPTY_SET = new HashSet<String>();
  private Operator oper;
  private List<Filter> filters;

  public FilterList(Operator oper) {
    super();
    this.oper = oper;
  }

  public Operator getOper() {
    return oper;
  }

  public List<Filter> getFilters() {
    if (filters != null)
      return filters;
    else
      return EMPTY_LIST;
  }

  public void addFilter(Filter filter) {
    if (filters == null)
      filters = new ArrayList<>();
    filters.add(filter);
  }

  @Override
  public String[] getColumnNames() {
    String[] result = EMPTY_ARRAY;
    List<Filter> filters = getFilters();
    if (filters.size() > 0) {
      List<String> names = new ArrayList<>();
      for (Filter filter : getFilters()) {
        names.addAll(filter.getColumnKeys());
      }
      result = new String[names.size()];
      names.toArray(result);
    }
    return result;
  }

  @Override
  public Set<String> getColumnKeys() {
    Set<String> result = EMPTY_SET;
    List<Filter> filters = getFilters();
    if (filters.size() > 0) {
      Set<String> names = new HashSet<>();
      for (Filter filter : getFilters()) {
        names.addAll(filter.getColumnKeys());
      }
      result = names;
    }
    return result;
  }

}
