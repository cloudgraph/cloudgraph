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

import org.plasma.query.model.Literal;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;

/**
 * Contains default functionality for <a href=
 * "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
 * >relational</a> binary query expressions.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * @see Expr
 * @see ExprVisitor
 * @see EvaluationContext
 */
public class DefaultRelationalBinaryExpr extends DefaultBinaryExpr implements RelationalBinaryExpr {
  protected Property property;
  protected String propertyPath;
  protected Literal literal;
  protected RelationalOperator operator;

  /**
   * Constructs a composite expression based on the given terms and <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
   * >relational</a> operator.
   * 
   * @param left
   *          the "left" expression term
   * @param right
   *          the "right" expression term
   * @param oper
   *          the logical operator
   */
  public DefaultRelationalBinaryExpr(Property property, Literal literal, RelationalOperator operator) {
    super(property, literal);
    if (property == null)
      throw new IllegalArgumentException("expected arg 'property'");
    if (literal == null)
      throw new IllegalArgumentException("expected arg 'literal'");
    this.property = property;
    this.literal = literal;
    this.operator = operator;
    this.propertyPath = createPropertyPath(this.property);
  }

  /**
   * Returns a "truth" value for the expression based on an evaluation of the <a
   * href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
   * >relational</a> operator within the given context.
   * 
   * @param context
   *          the context
   * @return "truth" value for the expression based on an evaluation of the <a
   *         href=
   *         "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
   *         >relational</a> operator within the given context.
   */
  @Override
  public boolean evaluate(EvaluationContext context) {
    return true;
  }

  /**
   * Returns the operator for the expression.
   * 
   * @return the operator for the expression.
   */
  @Override
  public RelationalOperator getOperator() {
    return this.operator;
  }

  @Override
  public Property getProperty() {
    return property;
  }

  @Override
  public String getPropertyPath() {
    return propertyPath;
  }

  @Override
  public Literal getLiteral() {
    return literal;
  }

  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(this.getClass().getSimpleName());
    buf.append(" [");
    buf.append(this.propertyPath);
    buf.append(" ");
    buf.append(this.operator.getValue().name());
    buf.append(" ");
    buf.append(this.literal.getValue());
    buf.append("]");
    return buf.toString();
  }

}
