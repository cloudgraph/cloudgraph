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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.query.expr.DefaultPredicateBinaryExpr;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.query.expr.PredicateBinaryExpr;
import org.plasma.query.model.Literal;
import org.plasma.query.model.PredicateOperator;
import org.plasma.query.model.Property;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaType;

/**
 * An {@link PredicateBinaryExpr} implementation which uses a specific
 * evaluation {@link GraphRecognizerContext context} to evaluate the value(s) of
 * a data graph property along the query property traversal path within the
 * context of a binary expression (syntax) tree.
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see GraphRecognizerContext
 * @see GraphRecognizerSupport
 */
public class GraphRecognizerPredicateBinaryExpr extends DefaultPredicateBinaryExpr implements
    PredicateBinaryExpr {
  private static Log log = LogFactory.getLog(GraphRecognizerPredicateBinaryExpr.class);
  protected Endpoint endpoint;
  protected GraphRecognizerSupport recognizer = new GraphRecognizerSupport();

  /**
   * Constructs an expression based on the given terms and column qualifier
   * prefix.
   * 
   * @param property
   *          the "left" property term
   * @param columnQualifierPrefix
   *          the qualifier prefix used to evaluate the expression for a given
   *          context.
   * @param literal
   *          the "right" literal term
   * @param operator
   *          the relational operator
   * @see EdgeRecognizerContext
   */
  public GraphRecognizerPredicateBinaryExpr(Property property, Literal literal,
      PredicateOperator operator) {
    super(property, literal, operator);
  }

  /**
   * Returns a "truth" value for the expression using a specific evaluation
   * {@link GraphRecognizerContext context} by evaluate the value of a property
   * associated with the evaluation {@link GraphRecognizerContext context}
   * within the binary expression tree.
   * 
   * @param context
   * @return a "truth" value for the expression using a specific evaluation
   *         {@link GraphRecognizerContext context} by evaluate the value of a
   *         property associated with the evaluation
   *         {@link GraphRecognizerContext context} within the binary expression
   *         tree.
   * @see GraphRecognizerContext
   */
  @Override
  public boolean evaluate(EvaluationContext context) {
    GraphRecognizerContext ctx = (GraphRecognizerContext) context;

    PlasmaDataGraph graph = ctx.getGraph();

    if (this.endpoint == null)
      this.endpoint = this.recognizer.getEndpoint(this.property, (PlasmaType) graph.getRootObject()
          .getType());

    List<Object> values = new ArrayList<Object>();
    this.recognizer.collect(graph.getRootObject(), this.property, this.property.getPath(), 0,
        values);
    for (Object value : values) {
      if (this.recognizer.evaluate(this.endpoint, value, this.operator.getValue(), this.literal)) {
        if (log.isDebugEnabled())
          log.debug(this.toString() + " evaluate true: " + String.valueOf(value));
        return true;
      } else {
        if (log.isDebugEnabled())
          log.debug(this.toString() + " evaluate false: " + String.valueOf(value));
      }
    }

    return false;
  }
}
