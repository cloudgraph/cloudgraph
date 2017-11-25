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
package org.cloudgraph.hbase.graph;

import org.cloudgraph.hbase.expr.PathPredicateBinaryExprTreeAssembler;
import org.cloudgraph.query.expr.DefaultBinaryExprTreeAssembler;
import org.cloudgraph.query.expr.DefaultLogicalBinaryExpr;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprAssembler;
import org.cloudgraph.query.expr.LogicalBinaryExpr;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.cloudgraph.query.expr.WildcardBinaryExpr;
import org.cloudgraph.store.lang.GraphFilterException;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.plasma.query.model.Literal;
import org.plasma.query.model.LogicalOperator;
import org.plasma.query.model.Path;
import org.plasma.query.model.PredicateOperator;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * A binary expression tree assembler which constructs an operator precedence
 * map, then {@link org.cloudgraph.query.expr.ExpresionVisitorSupport visits}
 * (traverses) the given predicate expression syntax tree depth-first using an
 * adapted shunting-yard algorithm and assembles a resulting binary tree
 * structure. In typical usage scenarios, a single expression tree is assembled
 * once, and then used to evaluate any number of graph edge or other results
 * based on a given context.
 * <p>
 * The adapted shunting-yard algorithm in general uses a stack of operators and
 * operands, and as new binary tree nodes are detected and created they are
 * pushed onto the operand stack based on operator precedence. The resulting
 * binary expression tree reflects the syntax of the underlying query expression
 * including the precedence of its operators.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 1.0.4
 * 
 * @see ExternalEdgeRecognizerRelationalBinaryExpr
 * @see ExternalEdgeRecognizerWildcardBinaryExpr
 * @see ExprAssembler
 */
public class ExternalEdgeRecognizerSyntaxTreeAssembler extends DefaultBinaryExprTreeAssembler {
  protected DataGraphMapping graphConfig;
  protected PlasmaType edgeType;

  /**
   * Constructs an assembler based on the given predicate and graph edge type.
   * 
   * @param predicate
   *          the predicate
   * @param edgeType
   *          the graph edge type which is the type for the reference property
   *          within the graph which represents an edge
   * @param rootType
   *          the graph root type
   * @param graphConfig
   *          the graph config
   */
  public ExternalEdgeRecognizerSyntaxTreeAssembler(Where predicate, DataGraphMapping graphConfig,
      PlasmaType edgeType, PlasmaType rootType) {
    super(predicate, rootType);
    this.graphConfig = graphConfig;
    this.edgeType = edgeType;
  }

  @Override
  public RelationalBinaryExpr createRelationalBinaryExpr(Property property, Literal literal,
      RelationalOperator operator) {
    return new ExternalEdgeRecognizerRelationalBinaryExpr(property, literal, operator);
  }

  @Override
  public WildcardBinaryExpr createWildcardBinaryExpr(Property property, Literal literal,
      PredicateOperator operator) {
    return new ExternalEdgeRecognizerWildcardBinaryExpr(property, literal, operator);
  }

  @Override
  public LogicalBinaryExpr createLogicalBinaryExpr(Expr left, Expr right, LogicalOperator operator) {
    return new DefaultLogicalBinaryExpr(left, right, operator);
  }

  /**
   * Process the traversal end event for a query
   * {@link org.plasma.query.model.Property property} within an
   * {@link org.plasma.query.model.Expression expression} setting up context
   * information for the endpoint property and its type, as well as physical
   * column qualifier name bytes which are set into the
   * {@link #contextQueryProperty} physical name bytes. for the current
   * {@link org.plasma.query.model.Expression expression}.
   * 
   * @see org.plasma.query.visitor.DefaultQueryVisitor#end(org.plasma.query.model.Property)
   */
  @Override
  public void end(Property property) {
    Path path = property.getPath();
    PlasmaType targetType = (PlasmaType) this.edgeType;
    if (path == null) {
      // no error here for external edges
    } else {
      targetType = traverse(path, targetType);
    }
    PlasmaProperty endpointProp = (PlasmaProperty) targetType.getProperty(property.getName());
    this.contextProperty = endpointProp;
    this.contextType = targetType;
    this.contextQueryProperty = property;

  }
}
