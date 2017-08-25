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

import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.hbase.expr.PathPredicateBinaryExprTreeAssembler;
import org.cloudgraph.query.expr.DefaultLogicalBinaryExpr;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprAssembler;
import org.cloudgraph.query.expr.LogicalBinaryExpr;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.cloudgraph.query.expr.WildcardBinaryExpr;
import org.plasma.query.model.Literal;
import org.plasma.query.model.LogicalOperator;
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
 * <p>
 * The use of binary expression tree evaluation for post processing of graph
 * edge results is necessary in columnar data stores, as an entity with multiple
 * properties is necessarily persisted across multiple columns. And while these
 * data stores provide many useful column oriented filters, the capability to
 * select an entity based on complex criteria which spans several columns is
 * generally not supported, as such filters are column oriented. Yet even for
 * simple queries (e.g. "where entity.c1 = 'foo' and entity.c2 = 'bar'") column
 * c1 and its value exists in one cell and column c2 exists in another table
 * cell. Since columnar data store filters cannot generally span columns, both
 * cells must be returned and the results post processed within the context of
 * the binary expression tree.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * 
 * @see LocalEdgeRecognizerRelationalBinaryExpr
 * @see LocalEdgeRecognizerWildcardBinaryExpr
 * @see ExprAssembler
 */
public class LocalEdgeRecognizerSyntaxTreeAssembler extends PathPredicateBinaryExprTreeAssembler {
  protected DataGraphConfig graphConfig;

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
  public LocalEdgeRecognizerSyntaxTreeAssembler(Where predicate, DataGraphConfig graphConfig,
      PlasmaType edgeType, PlasmaType rootType) {
    super(predicate, edgeType, rootType);
    this.graphConfig = graphConfig;
  }

  @Override
  public RelationalBinaryExpr createRelationalBinaryExpr(Property property, Literal literal,
      RelationalOperator operator) {

    String qual = Bytes.toString(this.contextQueryProperty.getPhysicalNameBytes());
    String delim = this.graphConfig.getColumnKeySequenceDelimiter();
    String qualPrefix = qual + delim;
    return new LocalEdgeRecognizerRelationalBinaryExpr(property, qualPrefix, literal, operator);
  }

  @Override
  public WildcardBinaryExpr createWildcardBinaryExpr(Property property, Literal literal,
      PredicateOperator operator) {
    String qual = Bytes.toString(this.contextQueryProperty.getPhysicalNameBytes());
    String delim = this.graphConfig.getColumnKeySequenceDelimiter();
    String qualPrefix = qual + delim;
    return new LocalEdgeRecognizerWildcardBinaryExpr(property, qualPrefix, literal, operator);
  }

  @Override
  public LogicalBinaryExpr createLogicalBinaryExpr(Expr left, Expr right, LogicalOperator operator) {
    return new DefaultLogicalBinaryExpr(left, right, operator);
  }

}
