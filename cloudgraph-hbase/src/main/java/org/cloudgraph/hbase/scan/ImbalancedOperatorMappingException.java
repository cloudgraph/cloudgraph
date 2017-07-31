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
import org.plasma.query.model.LogicalOperatorValues;
import org.plasma.query.model.RelationalOperatorValues;

/**
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class ImbalancedOperatorMappingException extends ScanException {
  private static final long serialVersionUID = 1L;

  public ImbalancedOperatorMappingException() {
    super();
  }

  public ImbalancedOperatorMappingException(String msg) {
    super(msg);
  }

  public ImbalancedOperatorMappingException(Throwable t) {
    super(t);
  }

  public ImbalancedOperatorMappingException(RelationalOperatorValues left,
      LogicalOperatorValues operator, RelationalOperatorValues right,
      UserDefinedRowKeyFieldConfig field) {
    super("relational operator '" + left + "' linked through logical operator '" + operator
        + "' to relational operator '" + right + "' for row key field property, "
        + field.getEndpointProperty().getContainingType().toString() + "."
        + field.getEndpointProperty().getName());
  }

}
