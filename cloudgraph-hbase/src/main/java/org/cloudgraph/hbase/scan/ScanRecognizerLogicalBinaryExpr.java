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
package org.cloudgraph.hbase.scan;

import org.cloudgraph.query.expr.DefaultLogicalBinaryExpr;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprVisitor;
import org.cloudgraph.query.expr.LogicalBinaryExpr;
import org.plasma.query.model.LogicalOperator;

/**
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see Expr
 * @see ExprVisitor
 * @see EvaluationContext
 */
public class ScanRecognizerLogicalBinaryExpr extends DefaultLogicalBinaryExpr implements
    LogicalBinaryExpr {
  /**
   * Constructs a composite expression based on the given terms and <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
   * >logical</a> operator.
   * 
   * @param left
   *          the "left" expression term
   * @param right
   *          the "right" expression term
   * @param oper
   *          the logical operator
   */
  public ScanRecognizerLogicalBinaryExpr(Expr left, Expr right, LogicalOperator oper) {
    super(left, right, oper);
  }

}