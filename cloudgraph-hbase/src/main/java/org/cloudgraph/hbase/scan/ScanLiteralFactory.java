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

import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.model.LogicalOperatorName;
import org.plasma.query.model.PredicateOperator;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Simple factory constructing data "flavor" and data type specific scan
 * literals given various configuration specific as well as predicate context
 * specific relational operators.
 * 
 * @see ScanLiteral
 * @author Scott Cinnamond
 * @since 0.5
 */
public class ScanLiteralFactory {

  /**
   * Creates and returns a data "flavor" and data type specific scan literal
   * given various configuration specific as well as predicate context specific
   * relational and logical operators.
   * 
   * @param content
   *          the literal string content
   * @param property
   *          the context property
   * @param rootType
   *          the graph root type
   * @param relationalOperator
   *          the context relational operator
   * @param fieldConfig
   *          the row-key field configuration
   * @return the data "flavor" and data type specific scan literal given various
   *         configuration specific as well as predicate context specific
   *         relational and logical operators.
   */
  public ScanLiteral createLiteral(String content, PlasmaProperty property, PlasmaType rootType,
      RelationalOperator relationalOperator, LogicalOperatorName logicalOperatorContext,
      DataRowKeyFieldMapping fieldConfig, StoreMappingContext mappingContext) {

    ScanLiteral result = null;
    DataType dataType = DataType.valueOf(property.getType().getName());

    switch (property.getDataFlavor()) {
    case integral:
      result = new IntegralLiteral(content, rootType, relationalOperator.getValue(),
          logicalOperatorContext, fieldConfig, mappingContext);
      break;
    case string:
      result = new StringLiteral(content, rootType, relationalOperator.getValue(),
          logicalOperatorContext, fieldConfig, mappingContext);
      break;
    case real:
      result = new RealLiteral(content, rootType, relationalOperator.getValue(),
          logicalOperatorContext, fieldConfig, mappingContext);
      break;
    case temporal:
      switch (dataType) {
      case Date:
      case DateTime:
      default:
        result = new TemporalLiteral(content, rootType, relationalOperator.getValue(),
            logicalOperatorContext, fieldConfig, mappingContext);
      }
      break;
    case other:
      throw new ScanException("data flavor '" + property.getDataFlavor()
          + "' not supported for relational operator, '" + relationalOperator + "'");
    }
    return result;
  }

  /**
   * Creates and returns a data "flavor" and data type specific scan literal
   * given various configuration specific as well as predicate context specific
   * predicate and logical operators.
   * 
   * @param content
   *          the literal string content
   * @param property
   *          the context property
   * @param rootType
   *          the graph root type
   * @param predicateOperator
   *          the context predicate operator
   * @param fieldConfig
   *          the row-key field configuration
   * @return the data "flavor" and data type specific scan literal given various
   *         configuration specific as well as predicate context specific
   *         relational and logical operators.
   */
  public ScanLiteral createLiteral(String content, PlasmaProperty property, PlasmaType rootType,
      PredicateOperator predicateOperator, LogicalOperatorName logicalOperatorContext,
      DataRowKeyFieldMapping fieldConfig, StoreMappingContext mappingContext) {

    ScanLiteral result = null;
    DataType dataType = DataType.valueOf(property.getType().getName());
    switch (predicateOperator.getValue()) {
    case LIKE:
      switch (property.getDataFlavor()) {
      case string:
        result = new WildcardStringLiteral(content, rootType, predicateOperator,
            logicalOperatorContext, fieldConfig, mappingContext);
        break;
      case integral:
      case real:
      case temporal:
      case other:
        throw new ScanException("data flavor '" + property.getDataFlavor()
            + "' not supported for predicate operator, '" + predicateOperator.getValue() + "'");
      }
      break;
    case IN:
      switch (property.getDataFlavor()) {
      case integral:
        result = new IntegralLiteral(content, rootType, RelationalOperatorName.EQUALS,
            logicalOperatorContext, fieldConfig, mappingContext);
        break;
      case string:
        result = new StringLiteral(content, rootType, RelationalOperatorName.EQUALS,
            logicalOperatorContext, fieldConfig, mappingContext);
        break;
      case real:
        result = new RealLiteral(content, rootType, RelationalOperatorName.EQUALS,
            logicalOperatorContext, fieldConfig, mappingContext);
        break;
      case temporal:
        switch (dataType) {
        case Date:
        case DateTime:
        default:
          result = new TemporalLiteral(content, rootType, RelationalOperatorName.EQUALS,
              logicalOperatorContext, fieldConfig, mappingContext);
        }
        break;
      default:
        throw new ScanException("data flavor '" + property.getDataFlavor()
            + "' not supported for predicate operator, '" + predicateOperator.getValue() + "'");
      }

      break;
    default:
      throw new ScanException("unsupported predicate operator '" + predicateOperator.getValue()
          + "'");

    }
    return result;
  }
}
