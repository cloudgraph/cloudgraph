/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.hbase.scan;

import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.WildcardOperator;
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
      RelationalOperator relationalOperator, UserDefinedRowKeyFieldConfig fieldConfig) {

    ScanLiteral result = null;
    DataType dataType = DataType.valueOf(property.getType().getName());

    switch (property.getDataFlavor()) {
    case integral:
      result = new IntegralLiteral(content, rootType, relationalOperator, fieldConfig);
      break;
    case string:
      result = new StringLiteral(content, rootType, relationalOperator, fieldConfig);
      break;
    case real:
      result = new RealLiteral(content, rootType, relationalOperator, fieldConfig);
      break;
    case temporal:
      switch (dataType) {
      case Date:
      case DateTime:
      default:
        result = new TemporalLiteral(content, rootType, relationalOperator, fieldConfig);
      }
      break;
    case other:
      throw new ScanException("data flavor '" + property.getDataFlavor()
          + "' not supported for relational operator, '" + relationalOperator.getValue() + "'");
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
      WildcardOperator wildcardOperator, UserDefinedRowKeyFieldConfig fieldConfig) {

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
