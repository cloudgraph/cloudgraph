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
package org.cloudgraph.recognizer;

import org.cloudgraph.query.expr.DefaultBinaryExprTreeAssembler;
import org.cloudgraph.query.expr.ExprAssembler;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.cloudgraph.query.expr.PredicateBinaryExpr;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.plasma.query.model.Literal;
import org.plasma.query.model.PredicateOperator;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * A binary expression tree assembler which constructs an operator precedence
 * map, then {@link org.cloudgraph.query.expr.ExpresionVisitorSupport visits}
 * (traverses) the given predicate expression syntax tree depth-first using an
 * adapted shunting-yard algorithm and assembles a resulting binary tree
 * structure with expression nodes specific for detecting i.e. recognizing a
 * data graph. In typical usage scenarios, a single expression tree is assembled
 * once, and then used to evaluate any number of data graphs.
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
 */
public class GraphRecognizerSyntaxTreeAssembler extends DefaultBinaryExprTreeAssembler {
  protected DataGraphMapping graphConfig;

  /**
   * Constructs an assembler based on the given predicate data graph
   * configuration and graph root type.
   * 
   * @param predicate
   *          the predicate
   * @param rootType
   *          the graph root type
   */
  public GraphRecognizerSyntaxTreeAssembler(Where predicate, PlasmaType rootType) {
    super(predicate, rootType);
  }

  @Override
  public RelationalBinaryExpr createRelationalBinaryExpr(Property property, Literal literal,
      RelationalOperator operator) {
    return new GraphRecognizerRelationalBinaryExpr(property, literal, operator);
  }

  @Override
  public PredicateBinaryExpr createPredicateBinaryExpr(Property property, Literal literal,
      PredicateOperator operator) {
    return new GraphRecognizerPredicateBinaryExpr(property, literal, operator);
  }
}
