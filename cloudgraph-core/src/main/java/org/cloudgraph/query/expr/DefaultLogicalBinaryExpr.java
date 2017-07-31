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
package org.cloudgraph.query.expr;

import org.plasma.query.model.LogicalOperator;

/**
 * Contains default functionality for <a href=
 * "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
 * >logical</a> binary query expressions.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * @see Expr
 * @see ExprVisitor
 * @see EvaluationContext
 */
public class DefaultLogicalBinaryExpr extends DefaultBinaryExpr implements LogicalBinaryExpr {
  private Expr left;
  private Expr right;
  private LogicalOperator oper;

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
  public DefaultLogicalBinaryExpr(Expr left, Expr right, LogicalOperator oper) {
    super(left, right);
    if (oper == null)
      throw new IllegalArgumentException("expected arg 'oper'");
    this.left = left;
    this.right = right;
    this.oper = oper;
  }

  /**
   * Returns the operator for the expression.
   * 
   * @return the operator for the expression.
   */
  public LogicalOperator getOperator() {
    return oper;
  }

  /**
   * Returns a "truth" value for the expression based on an evaluation of the <a
   * href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
   * >logical</a> operator within the given context.
   * 
   * @param context
   *          the context
   * @return "truth" value for the expression based on an evaluation of the <a
   *         href=
   *         "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
   *         >logical</a> operator within the given context.
   */
  @Override
  public boolean evaluate(EvaluationContext context) {
    boolean leftTrue = left.evaluate(context);
    boolean rightTrue = right.evaluate(context);

    boolean result = false;
    switch (oper.getValue()) {
    case AND:
      result = leftTrue && rightTrue;
      break;
    case OR:
      result = leftTrue || rightTrue;
      break;
    case NOT:
    default:
      throw new IllegalStateException("unexpected logical operator, " + oper.getValue());
    }
    return result;
  }

  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(this.getClass().getSimpleName());
    buf.append(" [");
    buf.append(this.oper.getValue().name());
    buf.append("]");
    return buf.toString();
  }
}