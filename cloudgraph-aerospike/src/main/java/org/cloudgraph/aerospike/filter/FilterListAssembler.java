/**
 * Copyright 2017 TerraMeta Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudgraph.aerospike.filter;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.filter.Filter;
//import org.apache.hadoop.hbase.filter.FilterList;
import org.plasma.query.model.NullLiteral;
import org.plasma.sdo.PlasmaType;

/**
 * Supports assembly of HBase filter lists.
 * <p>
 * HBase filters may be collected into lists using <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html"
 * target="#">FilterList</a> each with a <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ALL"
 * target="#">MUST_PASS_ALL</a> or <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ONE"
 * target="#">MUST_PASS_ONE</a> (logical) operator. Lists may then be assembled
 * into hierarchies used to represent complex expression trees filtering either
 * rows or columns in HBase.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public abstract class FilterListAssembler implements AerospikeFilterAssembler {
  private static Log log = LogFactory.getLog(FilterListAssembler.class);

  protected List<Object> params;
  // protected FilterList rootFilter;
  protected PlasmaType rootType;

  @SuppressWarnings("unused")
  private FilterListAssembler() {
  }

  protected FilterListAssembler(PlasmaType rootType) {
    this.rootType = rootType;

    // this.rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
  }

  /**
   * Returns the assembled filter or filter list root.
   * 
   * @return the assembled filter or filter list root.
   */
  public Filter getFilter() {
    return null;
    // return rootFilter;
  }

  public void clear() {
    if (params != null)
      params.clear();
    // this.rootFilter.getFilters().clear();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.filter.HBaseFilterAssembler#getParams()
   */
  public Object[] getParams() {
    Object[] result = new Object[params.size()];
    Iterator<Object> iter = params.iterator();
    for (int i = 0; iter.hasNext(); i++) {
      Object param = iter.next();
      if (!(param instanceof NullLiteral))
        result[i] = param;
      else
        result[i] = null;
    }
    return result;
  }

}
