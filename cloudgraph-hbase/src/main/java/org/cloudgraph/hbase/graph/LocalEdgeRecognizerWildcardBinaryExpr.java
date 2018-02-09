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
import org.apache.hadoop.hbase.KeyValue;
import org.cloudgraph.query.expr.DefaultWildcardBinaryExpr;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.query.expr.PredicateBinaryExpr;
import org.plasma.query.model.Literal;
import org.plasma.query.model.PredicateOperator;
import org.plasma.query.model.Property;

/**
 * An {@link PredicateBinaryExpr} implementation which uses a specific
 * evaluation {@link LocalEdgeRecognizerContext context} to locate or recognize
 * a given sequence based column qualifier within the context of the expression.
 * 
 * <p>
 * A sequence uniquely identifies an data graph entity within a local data graph
 * and is used to construct qualifier filters to return specific sequence-based
 * column qualifiers and associated column values.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * @see LocalEdgeRecognizerContext
 */
public class LocalEdgeRecognizerWildcardBinaryExpr extends DefaultWildcardBinaryExpr implements
    PredicateBinaryExpr {
  private static Log log = LogFactory.getLog(LocalEdgeRecognizerWildcardBinaryExpr.class);
  private String columnQualifierPrefix;

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
   *          the wildcard operator
   * @see LocalEdgeRecognizerContext
   */
  public LocalEdgeRecognizerWildcardBinaryExpr(Property property, String columnQualifierPrefix,
      Literal literal, PredicateOperator operator) {
    super(property, literal, operator);
    this.columnQualifierPrefix = columnQualifierPrefix;
  }

  /**
   * Returns a "truth" value for the expression using a specific evaluation
   * {@link LocalEdgeRecognizerContext context} to locate or recognize a given
   * sequence based column qualifier within the context of the expression.
   * 
   * @param context
   * @return a "truth" value for the expression using a specific evaluation
   *         {@link LocalEdgeRecognizerContext context} to locate or recognize a
   *         given sequence based column qualifier within the context of the
   *         expression.
   * @see LocalEdgeRecognizerContext
   */
  @Override
  public boolean evaluate(EvaluationContext context) {
    LocalEdgeRecognizerContext ctx = (LocalEdgeRecognizerContext) context;
    String qualifier = this.columnQualifierPrefix + String.valueOf(ctx.getSequence());
    KeyValue value = ctx.getKeyMap().get(qualifier);
    boolean found = value != null;
    if (log.isDebugEnabled())
      log.debug("evaluate: " + found + " '" + qualifier + "' in map ");
    return found;
  }

}
