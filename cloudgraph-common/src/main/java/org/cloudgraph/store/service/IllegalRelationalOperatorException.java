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
package org.cloudgraph.store.service;

import org.plasma.query.model.RelationalOperatorValues;

/**
 * @author Scott Cinnamond
 * @since 0.5
 */
public class IllegalRelationalOperatorException extends GraphServiceException {
  private static final long serialVersionUID = 1L;

  public IllegalRelationalOperatorException(RelationalOperatorValues operator, int fieldSeqNum,
      String fieldPath) {
    super(
        "relational operator ("
            + toUserString(operator)
            + ") not allowed for user "
            + "defined row key fields with a hash algorithm applied - see configured hash settings for "
            + "user defined field (" + fieldSeqNum + ") with path '" + fieldPath + "'");
  }

  public IllegalRelationalOperatorException(String message) {
    super(message);
  }

  public IllegalRelationalOperatorException(Throwable t) {
    super(t);
  }

  private static String toUserString(RelationalOperatorValues operator) {
    switch (operator) {
    case EQUALS:
      return "equals";
    case NOT_EQUALS:
      return "not equals";
    case GREATER_THAN:
      return "greater than";
    case GREATER_THAN_EQUALS:
      return "greater than equals";
    case LESS_THAN:
      return "less than";
    case LESS_THAN_EQUALS:
      return "less than equals";
    default:
      return operator.name();
    }
  }

}