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

import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.query.Term;
import org.plasma.query.model.AbstractPathElement;
import org.plasma.query.model.PathElement;
import org.plasma.query.model.PathNode;
import org.plasma.query.model.Property;
import org.plasma.query.model.WildcardPathElement;
import org.plasma.sdo.core.CoreType;

/**
 * Contains default functionality for query expressions composed of two parts or
 * terms, including a visitor traversal implementation.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * @see Term
 * @see ExprVisitor
 * @see EvaluationContext
 */
public abstract class DefaultBinaryExpr extends DefaultExpr implements BinaryExpr {
  private static Log log = LogFactory.getLog(DefaultBinaryExpr.class);

  /**
   * Constructs an expression using the given terms
   * 
   * @param left
   *          the "left" expression term
   * @param right
   *          the "right" expression term
   */
  public DefaultBinaryExpr(Term left, Term right) {
    super();
    if (left == null)
      throw new IllegalArgumentException("expected arg 'left'");
    if (right == null)
      throw new IllegalArgumentException("expected arg 'right'");
    this.left = left;
    this.right = right;
  }

  private Term left;
  private Term right;

  @Override
  public Term getLeft() {
    return left;
  }

  public void setLeft(Term left) {
    this.left = left;
  }

  @Override
  public Term getRight() {
    return right;
  }

  public void setRight(Term right) {
    this.right = right;
  }

  /**
   * Returns a "truth" value for the expression based on the given context.
   * 
   * @param context
   * @return a "truth" value for the expression based on the given context.
   */
  @Override
  public boolean evaluate(EvaluationContext context) {
    return true;
  }

  /**
   * Begins the traversal of the expression tree with this node as the root.
   * 
   * @param visitor
   *          the expression visitor
   */
  public void accept(ExprVisitor visitor) {
    accept(this, null, visitor, new HashSet<Expr>(), 0);
  }

  private void accept(Expr target, Expr source, ExprVisitor visitor, HashSet<Expr> visited,
      int level) {
    if (!visited.contains(target)) {
      visitor.visit(target, source, level);
      visited.add(target);
    } else
      return;
    if (DefaultBinaryExpr.class.isAssignableFrom(this.left.getClass())) {
      DefaultBinaryExpr expr = (DefaultBinaryExpr) this.left;
      expr.accept(expr, this, visitor, visited, level + 1);
    } else
      log.debug("ignoring left, " + this.left.getClass().getName());

    if (DefaultBinaryExpr.class.isAssignableFrom(this.right.getClass())) {
      DefaultBinaryExpr expr = (DefaultBinaryExpr) this.right;
      expr.accept(expr, this, visitor, visited, level + 1);
    } else
      log.debug("ignoring right, " + this.right.getClass().getName());
  }

  protected String createPropertyPath(Property property) {
    StringBuilder buf = new StringBuilder();
    if (property.getPath() != null) {
      for (PathNode node : property.getPath().getPathNodes()) {
        AbstractPathElement pathElem = node.getPathElement();
        if (pathElem instanceof PathElement) {
          buf.append(((PathElement) pathElem).getValue());
        } else if (pathElem instanceof WildcardPathElement) {
          buf.append(((WildcardPathElement) pathElem).getValue());
        } else {
          throw new IllegalStateException("unknown path element, " + pathElem.getClass().getName());
        }
        buf.append("/");
      }
    }
    buf.append(property.getName());
    return buf.toString();

  }
}
