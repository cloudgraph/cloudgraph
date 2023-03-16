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
package org.cloudgraph.rocksdb.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.filter.Filter;
//import org.apache.hadoop.hbase.filter.FilterList;
import org.cloudgraph.query.expr.ExpresionVisitorSupport;
import org.cloudgraph.rocksdb.ext.PredExp;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.model.NullLiteral;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;
import org.xml.sax.SAXException;

/**
 * Supports assembly of complex HBase filter hierarchies representing query
 * predicate expression trees using a filter stack.
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
public abstract class FilterHierarchyAssembler extends ExpresionVisitorSupport implements
    RocksDBFilterAssembler {
  private static Log log = LogFactory.getLog(FilterHierarchyAssembler.class);

  protected List<Object> params = new ArrayList<Object>();
  protected PredExp[] rootFilter;
  protected Stack<PredExp[]> filterStack = new Stack<PredExp[]>();
  protected PlasmaType rootType;
  protected Map<String, ColumnInfo> columnMap = new HashMap<>();

  @SuppressWarnings("unused")
  private FilterHierarchyAssembler() {
  }

  protected FilterHierarchyAssembler(PlasmaType rootType) {
    this.rootType = rootType;
  }

  public void clear() {
    if (this.params != null)
      params.clear();
    this.filterStack.clear();
  }

  /**
   * Returns the assembled filter, filter list or filter hierarchy root.
   * 
   * @return the assembled filter, filter list or or filter hierarchy root.
   */
  public Filter getFilter() {
    return new MultiColumnPrefixFilter(columnMap);
  }

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

  protected void pushFilter() {
    pushFilter(null); // FIXME:
  }

  protected void pushFilter(PredExp oper) {
    PredExp[] filter = null;
    PredExp[] top = null;
    if (this.filterStack.size() > 0) {
      top = this.filterStack.peek();
      // top.addFilter(filter);
    } else
      this.rootFilter = filter;
    this.filterStack.push(filter);
  }

  protected void popFilter() {
    this.filterStack.pop();
  }

  // String.split() can cause empty tokens under some circumstances
  protected String[] filterTokens(String[] tokens) {
    int count = 0;
    for (int i = 0; i < tokens.length; i++)
      if (tokens[i].length() > 0)
        count++;
    String[] result = new String[count];
    int j = 0;
    for (int i = 0; i < tokens.length; i++)
      if (tokens[i].length() > 0) {
        result[j] = tokens[i];
        j++;
      }
    return result;
  }

  protected void log(Where root) {
    String xml = "";
    PlasmaQueryDataBinding binding;
    try {
      binding = new PlasmaQueryDataBinding(new DefaultValidationEventHandler());
      xml = binding.marshal(root);
    } catch (JAXBException e) {
      log.debug(e);
    } catch (SAXException e) {
      log.debug(e);
    }
    log.debug("query: " + xml);
  }

}
