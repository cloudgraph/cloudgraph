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
package org.cloudgraph.hbase.scan;

import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.query.expr.DefaultBinaryExprTreeAssembler;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprAssembler;
import org.cloudgraph.query.expr.LogicalBinaryExpr;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.plasma.query.model.Literal;
import org.plasma.query.model.LogicalOperator;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * A binary expression tree assembler which constructs an operator precedence
 * map, then {@link org.cloudgraph.hbase.expr.ExpresionVisitorSupport visits}
 * (traverses) the given predicate expression syntax tree depth-first using an
 * adapted shunting-yard algorithm and assembles a resulting binary tree
 * structure with expression nodes specific for detecting i.e. recognizing one
 * or more {@link PartialRowKey partial}, {@link FuzzyRowKey fuzzy} and other
 * and other scan constructs. These are collected within the specific evaluation
 * {@link ScanRecognizerContext context} passed to the assembled syntax tree.
 * <p>
 * The adapted shunting-yard algorithm in general uses a stack of operators and
 * operands, and as new binary tree nodes are detected and created they are
 * pushed onto the operand stack based on operator precedence. The resulting
 * binary expression tree reflects the syntax of the underlying query expression
 * including the precedence of its operators.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * 
 * @see ExprAssembler
 * @see DefaultBinaryExprTreeAssembler
 * @see ScanRecognizerContext
 */
public class ScanRecognizerSyntaxTreeAssembler extends DefaultBinaryExprTreeAssembler {
  protected DataGraphConfig graphConfig;

  /**
   * Constructs an assembler based on the given predicate data graph
   * configuration and graph root type.
   * 
   * @param predicate
   *          the predicate
   * @param rootType
   *          the graph root type
   */
  public ScanRecognizerSyntaxTreeAssembler(Where predicate, PlasmaType rootType) {
    super(predicate, rootType);
  }

  /**
   * Creates and returns a relational binary expression based on the given terms
   * and <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
   * >relational</a> operator.
   * 
   * @param property
   *          the property term
   * @param literal
   *          the literal term
   * @param operator
   *          the <a href=
   *          "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
   *          >relational</a> operator
   * @return a relational binary expression based on the given terms and <a
   *         href=
   *         "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
   *         >relational</a> operator.
   */
  @Override
  public RelationalBinaryExpr createRelationalBinaryExpr(Property property, Literal literal,
      RelationalOperator operator) {
    return new ScanRecognizerRelationalBinaryExpr(property, literal, operator);
  }

  /**
   * Creates and returns a logical binary expression based on the given terms
   * and <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
   * >logical</a> operator.
   * 
   * @param property
   *          the property term
   * @param literal
   *          the literal term
   * @param operator
   *          the <a href=
   *          "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
   *          >logical</a> operator
   * @return a wildcard binary expression based on the given terms and <a href=
   *         "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
   *         >logical</a> operator.
   */
  @Override
  public LogicalBinaryExpr createLogicalBinaryExpr(Expr left, Expr right, LogicalOperator operator) {
    return new ScanRecognizerLogicalBinaryExpr(left, right, operator);
  }

}
