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