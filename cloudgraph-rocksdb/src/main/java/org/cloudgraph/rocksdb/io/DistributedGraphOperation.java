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
package org.cloudgraph.rocksdb.io;

import org.cloudgraph.rocksdb.connect.Connection;
import org.cloudgraph.store.mapping.StoreMappingContext;

/**
 * Encapsulates one or more graph table operation components for federation
 * across multiple physical tables and/or physical table rows.
 * 
 * @see org.cloudgraph.rocksdb.io.GraphTableReader
 * @see org.cloudgraph.rocksdb.io.GraphTableWriter
 * @see org.cloudgraph.state.GraphTable
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface DistributedGraphOperation {

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

  /** returns the shared connection for the operation */
  public Connection getConnection();

  public StoreMappingContext getMappingContext();

  /**
   * Closes tables and connections.
   */
  public void close();
}
