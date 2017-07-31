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

import org.apache.hadoop.hbase.client.Table;
import org.cloudgraph.state.TableState;

/**
 * The the pooled connection and other context for read or write operations for
 * a specific HBase table.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface TableOperation extends TableState {

  /**
   * Returns the HBase table pooled connection for this context.
   * 
   * @return the HBase table pooled connection for this context.
   */
  public Table getTable();

  /**
   * Returns whether there is an active HBase table pooled connection for this
   * context.
   * 
   * @return whether there is an active HBase table pooled connection for this
   *         context.
   */
  public boolean hasConnection();

  /**
   * Returns the distributed context associated with this table operation
   * context.
   * 
   * @return the distributed context associated with this table operation
   *         context.
   */
  public DistributedOperation getDistributedOperation();

  /**
   * Sets the distributed context associated with this table operation context.
   * 
   * @param distributedOperation
   *          the operation
   */
  // public void setDistributedOperation(DistributedOperation
  // distributedOperation);
}
