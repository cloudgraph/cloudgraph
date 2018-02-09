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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.query.expr.DefaultWildcardBinaryExpr;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.query.expr.PredicateBinaryExpr;
import org.cloudgraph.recognizer.Endpoint;
import org.cloudgraph.recognizer.GraphRecognizerSupport;
import org.plasma.query.model.Literal;
import org.plasma.query.model.PredicateOperator;
import org.plasma.query.model.Property;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * An {@link PredicateBinaryExpr} implementation which uses a specific
 * evaluation {@link ExternalEdgeRecognizerContext context} to locate or
 * recognize a given row key field within the context of the expression.
 * 
 * <p>
 * For external edges, we have only the row key fields to evaluate and the
 * predicate tree may contain predicates with properties which target fields
 * anywhere in the target graph including properties outside the row key. So for
 * external edge evaluators always return true when a value is not found.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 1.0.4
 * @see ExternalEdgeRecognizerContext
 */
public class ExternalEdgeRecognizerWildcardBinaryExpr extends DefaultWildcardBinaryExpr implements
    PredicateBinaryExpr {
  private static Log log = LogFactory.getLog(ExternalEdgeRecognizerWildcardBinaryExpr.class);
  protected Endpoint endpoint;
  protected GraphRecognizerSupport recognizer = new GraphRecognizerSupport();

  /**
   * Constructs an expression based on the given terms and column qualifier
   * prefix.
   * 
   * @param property
   *          the "left" property term
   * @param literal
   *          the "right" literal term
   * @param operator
   *          the wildcard operator
   * @see LocalEdgeRecognizerContext
   */
  public ExternalEdgeRecognizerWildcardBinaryExpr(Property property, Literal literal,
      PredicateOperator operator) {
    super(property, literal, operator);
  }

  /**
   * Returns a "truth" value for the expression using a specific evaluation
   * {@link ExternalEdgeRecognizerContext context} to locate or recognize a
   * given row key field within the context of the expression.
   * 
   * <p>
   * For external edges, we have only the row key fields to evaluate and the
   * predicate tree may contain predicates with properties which target fields
   * anywhere in the target graph including properties outside the row key. So
   * for external edge evaluators always return true when a value is not found,
   * but indicate the row evaluation was not complete so downstream actions can
   * be taken.
   * </p>
   * 
   * @param context
   * @return a "truth" value for the expression using a specific evaluation
   *         {@link ExternalEdgeRecognizerContext context} to locate or
   *         recognize a given row key field within the context of the
   *         expression.
   * @see LocalEdgeRecognizerContext
   */
  @Override
  public boolean evaluate(EvaluationContext context) {
    ExternalEdgeRecognizerContext ctx = (ExternalEdgeRecognizerContext) context;

    if (this.endpoint == null)
      this.endpoint = this.recognizer.getEndpoint(this.property, (PlasmaType) ctx.getContextType());

    Object rowKeyFieldValue = ctx.getValue(this.endpoint);
    if (rowKeyFieldValue != null)
      if (this.recognizer.evaluate(this.endpoint.getProperty(), rowKeyFieldValue,
          this.operator.getValue(), this.literal.getValue())) {
        if (log.isDebugEnabled())
          log.debug(this.toString() + " evaluate true: " + String.valueOf(rowKeyFieldValue));
        return true;
      } else {
        if (log.isDebugEnabled())
          log.debug(this.toString() + " evaluate false: " + String.valueOf(rowKeyFieldValue));
        return false;
      }
    // for external edges, we return true when the
    // row field is not found but indicate
    // the row evaluation was not complete.
    ctx.setRowEvaluatedCompletely(false);
    return true;
  }

}
