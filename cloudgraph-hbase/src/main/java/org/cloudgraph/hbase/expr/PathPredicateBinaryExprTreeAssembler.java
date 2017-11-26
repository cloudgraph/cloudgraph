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
package org.cloudgraph.hbase.expr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.key.CompositeColumnKeyFactory;
import org.cloudgraph.query.expr.DefaultBinaryExprTreeAssembler;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprAssembler;
import org.cloudgraph.store.lang.GraphFilterException;
import org.plasma.query.model.Path;
import org.plasma.query.model.Property;
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
 * <p>
 * Subclasses may provide alternate implementations of {@link ExprAssembler}
 * which create binary expression tree nodes with specific evaluation behavior.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * 
 * @see Expr
 * @see CompositeColumnKeyFactory
 * 
 */
public abstract class PathPredicateBinaryExprTreeAssembler extends DefaultBinaryExprTreeAssembler {
  private static Log log = LogFactory.getLog(PathPredicateBinaryExprTreeAssembler.class);

  protected CompositeColumnKeyFactory columnKeyFactory;
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
   */
  public PathPredicateBinaryExprTreeAssembler(Where predicate, PlasmaType edgeType,
      PlasmaType rootType) {
    super(predicate, rootType);
    this.edgeType = edgeType;
    this.columnKeyFactory = new CompositeColumnKeyFactory(this.rootType);

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
    if (path != null)
      throw new GraphFilterException(
          "property paths not supported within path predicate expressions");

    PlasmaProperty endpointProp = (PlasmaProperty) targetType.getProperty(property.getName());
    this.contextProperty = endpointProp;
    this.contextType = targetType;
    this.contextQueryProperty = property;
    byte[] columnKey = this.columnKeyFactory.createColumnKey(this.edgeType, this.contextProperty);
    this.contextQueryProperty.setPhysicalNameBytes(columnKey);
  }

}
