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
package org.cloudgraph.hbase.io;

/**
 * Thrown where an attempt is made to add a data object to a new or existing
 * edge where the edge base type or subtype have already been established and
 * the given type does not match.
 * 
 * @author Scott Cinnamond
 * @since 1.0.0
 */
public class EdgeTypeMismatchException extends OperationException {

  private static final long serialVersionUID = 1L;

  public EdgeTypeMismatchException() {
    super();
  }

  public EdgeTypeMismatchException(String msg) {
    super(msg);
  }

  public EdgeTypeMismatchException(Throwable t) {
    super(t);
  }
}
