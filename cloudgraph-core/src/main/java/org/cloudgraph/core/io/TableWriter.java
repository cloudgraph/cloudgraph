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
package org.cloudgraph.core.io;

import java.util.List;
import java.util.UUID;

import org.cloudgraph.core.ServiceContext;

/**
 * Provides access to the operational, configuration and other state information
 * required for write operations on a single graph table.
 * <p>
 * Acts as a container for one or more {@link RowWriter} elements and
 * encapsulates the HBase client <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html"
 * >Put</a> and <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Delete.html"
 * >Delete</a> operations for use in write operations across one or more graph
 * rows within a table.
 * </p>
 * 
 * @see org.cloudgraph.core.io.RowWriter
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface TableWriter extends TableOperation {

  public DistributedWriter getDistributedWriter();

  // public BufferedMutator getBufferedMutator();

  /**
   * 
   * Returns the qualified logical table name associated with this reader.
   * 
   * @return the qualified logical table name associated with this reader.
   */
  public String getQualifiedLogicalTableName();

  // /**
  // *
  // * Returns the physical table name associated with this reader.
  // *
  // * @return the physical table name associated with this reader.
  // */
  // public String getPhysicalTableName();

  /**
   * 
   * Returns the qualified physical table namespace associated with this reader.
   * 
   * @return the qualified physical table namespace associated with this reader.
   */
  public String getQualifiedPhysicalTableNamespace();

  /**
   * Return whether any concurrent rows have been detected for the table writer.
   * Optimization to enable bypassing of concurrent processing for an entire
   * table.
   * 
   * @return whether any concurrent rows have been detected for the table
   *         writer.
   */
  public boolean hasConcurrentRows();

  /**
   * Sets whether any concurrent rows have been detected for the table writer.
   * Optimization to enable bypassing of concurrent processing for an entire
   * table.
   */
  public void setHasConcurrentRows(boolean value);

  @Deprecated
  public void setDistributedWriter(DistributedWriter distributedGraphWriter);

  /**
   * Returns the row writer context for the given UUID
   * 
   * @param uuid
   *          the UUID string
   * @return the row writer context for the given UUID
   */
  public RowWriter getRowWriter(UUID uuid);

  /**
   * Adds the given row writer context mapping it to the given UUID.
   * 
   * @param uuid
   *          the UUID
   * @param rowContext
   *          the row writer context
   */
  public void addRowWriter(UUID uuid, RowWriter rowContext);

  /**
   * Returns all row writer context values for this table context.
   * 
   * @return all row writer context values for this table context.
   */
  public List<RowWriter> getAllRowWriters();

  public ServiceContext getServiceContext();

}
