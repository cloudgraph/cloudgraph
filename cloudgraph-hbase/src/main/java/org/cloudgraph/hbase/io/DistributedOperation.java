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

import org.cloudgraph.state.StateMarshalingContext;

/**
 * Encapsulates one or more graph table operation components for federation
 * across multiple physical tables and/or physical table rows.
 * 
 * @see org.cloudgraph.hbase.io.GraphTableReader
 * @see org.cloudgraph.hbase.io.GraphTableWriter
 * @see org.cloudgraph.state.GraphTable
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface DistributedOperation {

  /**
   * Returns true if only one table operation exists with only one associated
   * (root) type for this operation.
   * <p>
   * Note: because there may be a single root type within an operational
   * context, this does not mean there is no federation involved. A type is a
   * metadata entity, and there may be many (data object) instances of the type
   * within a graph operation.
   * </p>
   * 
   * @return true if only one table operation exists with only one associated
   *         (root) type for this operation.
   */
  public boolean hasSingleRootType();

  /**
   * Returns the marshalling context for this operation.
   * 
   * @return the marshalling context for this operation
   */
  public StateMarshalingContext getMarshallingContext();
}
