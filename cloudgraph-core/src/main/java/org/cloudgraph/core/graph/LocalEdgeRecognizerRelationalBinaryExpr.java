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
package org.cloudgraph.core.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.core.client.KeyValue;
//import org.apache.hadoop.hbase.KeyValue;
import org.cloudgraph.query.expr.DefaultRelationalBinaryExpr;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.plasma.query.model.Literal;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;

/**
 * An {@link RelationalBinaryExpr} implementation which uses a specific
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
public class LocalEdgeRecognizerRelationalBinaryExpr extends DefaultRelationalBinaryExpr implements
    RelationalBinaryExpr {
  private static Log log = LogFactory.getLog(LocalEdgeRecognizerRelationalBinaryExpr.class);
  private char[] columnQualifierPrefixChars;

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
   * @see LocalEdgeRecognizerContext
   */
  public LocalEdgeRecognizerRelationalBinaryExpr(Property property, String columnQualifierPrefix,
      Literal literal, RelationalOperator operator) {
    super(property, literal, operator);
    this.columnQualifierPrefixChars = columnQualifierPrefix.toCharArray();
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
    char[] seqChars = ctx.getSequence().toString().toCharArray();
    char[] qualifier = new char[this.columnQualifierPrefixChars.length + seqChars.length];
    System.arraycopy(this.columnQualifierPrefixChars, 0, qualifier, 0,
        this.columnQualifierPrefixChars.length);
    System.arraycopy(seqChars, 0, qualifier, this.columnQualifierPrefixChars.length,
        seqChars.length);
    KeyValue value = ctx.getKeyMap().get(new String(qualifier));
    boolean found = value != null;
    if (log.isDebugEnabled())
      log.debug("evaluate: " + found + " '" + qualifier + "' in map " + ctx.getKeyMap().keySet());
    return found;
  }

}
