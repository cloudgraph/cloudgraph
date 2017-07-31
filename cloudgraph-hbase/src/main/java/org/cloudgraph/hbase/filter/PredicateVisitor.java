/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
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
