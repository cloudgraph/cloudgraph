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
   * Returns the distributed context associated with this table operation
   * context.
   * 
   * @return the distributed context associated with this table operation
   *         context.
   */
  public DistributedGraphOperation getDistributedOperation();

  /**
   * Sets the distributed context associated with this table operation context.
   * 
   * @param distributedOperation
   *          the operation
   */
  // public void setDistributedOperation(DistributedOperation
  // distributedOperation);
  /**
   * Frees resources.
   */
  public void close();
}
