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
package org.cloudgraph.hbase.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.plasma.query.QueryException;
import org.plasma.query.model.GroupOperator;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.WildcardOperator;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.DataAccessException;

/**
 * Processes visitor events for query model elements common to both row and
 * column filters, such as relational and group operators, within the context of
 * HBase filter hierarchy assembly and maintains various context information
 * useful to subclasses.
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
public abstract class PredicateVisitor extends FilterHierarchyAssembler {
  private static Log log = LogFactory.getLog(PredicateVisitor.class);
  protected PlasmaType contextType;
  protected PlasmaProperty contextProperty;
  protected CompareFilter.CompareOp contextHBaseCompareOp;
  protected boolean contextOpWildcard;
  protected WildcardOperator contextWildcardOperator;

  protected PredicateVisitor(PlasmaType rootType) {
    super(rootType);
  }

  public void clear() {
    super.clear();
    this.contextType = null;
    this.contextProperty = null;
    this.contextHBaseCompareOp = null;
    this.contextOpWildcard = false;
    this.contextWildcardOperator = null;
  }

  public void start(RelationalOperator operator) {

    this.contextOpWildcard = false;
    this.contextWildcardOperator = null;

    switch (operator.getValue()) {
    case EQUALS:
      this.contextHBaseCompareOp = CompareFilter.CompareOp.EQUAL;
      break;
    case NOT_EQUALS:
      this.contextHBaseCompareOp = CompareFilter.CompareOp.NOT_EQUAL;
      break;
    case GREATER_THAN:
      this.contextHBaseCompareOp = CompareFilter.CompareOp.GREATER;
      break;
    case GREATER_THAN_EQUALS:
      this.contextHBaseCompareOp = CompareFilter.CompareOp.GREATER_OR_EQUAL;
      break;
    case LESS_THAN:
      this.contextHBaseCompareOp = CompareFilter.CompareOp.LESS;
      break;
    case LESS_THAN_EQUALS:
      this.contextHBaseCompareOp = CompareFilter.CompareOp.LESS_OR_EQUAL;
      break;
    default:
      throw new DataAccessException("unknown operator '" + operator.getValue().toString() + "'");
    }
    super.start(operator);
  }

  public void start(GroupOperator operator) {
    switch (operator.getValue()) {
    case RP_1:
      if (log.isDebugEnabled())
        log.debug("pushing expression filter");
      this.pushFilter();
      break;
    case RP_2:
      if (log.isDebugEnabled())
        log.debug("pushing 2 expression filters");
      this.pushFilter();
      this.pushFilter();
      break;
    case RP_3:
      if (log.isDebugEnabled())
        log.debug("pushing 3 expression filters");
      this.pushFilter();
      this.pushFilter();
      this.pushFilter();
      break;
    case LP_1:
      if (log.isDebugEnabled())
        log.debug("poping expression filter");
      this.popFilter();
      break;
    case LP_2:
      if (log.isDebugEnabled())
        log.debug("poping 2 expression filters");
      this.popFilter();
      this.popFilter();
      break;
    case LP_3:
      if (log.isDebugEnabled())
        log.debug("poping 3 expression filters");
      this.popFilter();
      this.popFilter();
      this.popFilter();
      break;
    default:
      throw new QueryException("unknown group operator, " + operator.getValue().name());
    }
    super.start(operator);
  }

}
