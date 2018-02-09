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

import org.cloudgraph.store.mapping.UserDefinedRowKeyFieldMapping;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.PredicateOperator;
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
      RelationalOperator relationalOperator, UserDefinedRowKeyFieldMapping fieldConfig) {

    ScanLiteral result = null;
    DataType dataType = DataType.valueOf(property.getType().getName());

    switch (property.getDataFlavor()) {
    case integral:
      result = new IntegralLiteral(content, rootType, relationalOperator.getValue(), fieldConfig);
      break;
    case string:
      result = new StringLiteral(content, rootType, relationalOperator.getValue(), fieldConfig);
      break;
    case real:
      result = new RealLiteral(content, rootType, relationalOperator.getValue(), fieldConfig);
      break;
    case temporal:
      switch (dataType) {
      case Date:
      case DateTime:
      default:
        result = new TemporalLiteral(content, rootType, relationalOperator.getValue(), fieldConfig);
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
   * wildcard and logical operators.
   * 
   * @param content
   *          the literal string content
   * @param property
   *          the context property
   * @param rootType
   *          the graph root type
   * @param wildcardOperator
   *          the context wildcard operator
   * @param fieldConfig
   *          the row-key field configuration
   * @return the data "flavor" and data type specific scan literal given various
   *         configuration specific as well as predicate context specific
   *         relational and logical operators.
   */
  public ScanLiteral createLiteral(String content, PlasmaProperty property, PlasmaType rootType,
      PredicateOperator wildcardOperator, UserDefinedRowKeyFieldMapping fieldConfig) {

    ScanLiteral result = null;
    DataType dataType = DataType.valueOf(property.getType().getName());

    switch (property.getDataFlavor()) {
    case string:
      result = new WildcardStringLiteral(content, rootType, wildcardOperator, fieldConfig);
      break;
    case integral:
    case real:
    case temporal:
    case other:
      throw new ScanException("data flavor '" + property.getDataFlavor()
          + "' not supported for wildcard operator, '" + wildcardOperator.getValue() + "'");
    }
    return result;
  }
}
