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
package org.cloudgraph.core.scan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.core.graph.LocalEdgeRecognizerContext;
import org.cloudgraph.query.expr.DefaultRelationalBinaryExpr;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.model.Literal;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * An {@link RelationalBinaryExpr} implementation which uses a specific
 * evaluation {@link ScanRecognizerContext context} to evaluate the value(s) of
 * a data graph property along the query property traversal path within the
 * context of a binary expression (syntax) tree.
 * 
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see ScanRecognizerContext
 */
public class ScanRecognizerRelationalBinaryExpr extends DefaultRelationalBinaryExpr implements
    RelationalBinaryExpr {
  private static Log log = LogFactory.getLog(ScanRecognizerRelationalBinaryExpr.class);
  protected ScanLiteral scanLiteral;
  protected StoreMappingContext mappingContext;

  /**
   * Constructs an expression based on the given terms.
   * 
   * @param property
   *          the "left" property term
   * @param literal
   *          the "right" literal term
   * @param operator
   *          the relational operator
   * @see LocalEdgeRecognizerContext
   */
  public ScanRecognizerRelationalBinaryExpr(Property property, Literal literal,
      RelationalOperator operator, StoreMappingContext mappingContext) {
    super(property, literal, operator);
    this.mappingContext = mappingContext;
  }

  /**
   * Returns a "truth" value for the expression using a specific evaluation
   * {@link ScanRecognizerContext context} by ... within the binary expression
   * tree.
   * 
   * @param context
   * @return a "truth" value for the expression
   * @see ScanRecognizerContext
   */
  @Override
  public boolean evaluate(EvaluationContext context) {
    ScanRecognizerContext ctx = (ScanRecognizerContext) context;
    ScanLiteral literal = createLiteral(ctx.getGraph());

    return false;
  }

  private ScanLiteral createLiteral(DataGraphMapping graph) {
    // Match the current property to a user defined
    // row key token, if found we can process
    DataRowKeyFieldMapping fieldConfig = graph.getUserDefinedRowKeyField(this.propertyPath);
    if (fieldConfig != null) {
      PlasmaProperty property = (PlasmaProperty) fieldConfig.getEndpointProperty();
      ScanLiteralFactory factory = new ScanLiteralFactory();

      ScanLiteral scanLiteral = factory.createLiteral(this.literal.getValue(), property,
          (PlasmaType) graph.getRootType(), this.operator, null, fieldConfig, this.mappingContext);
      return scanLiteral;
    } else
      log.warn("no user defined row-key field for query path '" + this.propertyPath
          + "' - deferring to graph recogniser post processor");

    return null;
  }

}
