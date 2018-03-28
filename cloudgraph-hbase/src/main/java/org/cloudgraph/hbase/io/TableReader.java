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

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import commonj.sdo.DataObject;

/**
 * Provides access to the operational, configuration and other state information
 * required for read operations on a single graph table.
 * <p>
 * Acts as a container for one or more {@link RowReader} elements and
 * encapsulates the HBase client <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Get.html"
 * >Get</a> operations for use in read operations across one or more graph rows.
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.RowReader
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface TableReader extends TableOperation {

  /**
   * Returns the table name associated with this reader.
   * 
   * @return the table name associated with this reader.
   */
  public String getTableName();

  /**
   * Returns the row reader context for the given UUID or null if null exists
   * 
   * @param uuid
   *          the UUID
   * @return the row reader context for the given UUID or null if null exists
   */
  public RowReader getRowReader(UUID uuid);

  /**
   * Returns the row reader context for the given row key or null if null exists
   * 
   * @param rowKey
   *          the row key bytes
   * @return the row reader context for the given row key or null if null exists
   */
  public RowReader getRowReader(byte[] rowKey);

  /**
   * Returns the row reader context for the given row key or null if null exists
   * 
   * @param rowKey
   *          the row key string
   * @return the row reader context for the given row key or null if null exists
   */
  public RowReader getRowReader(String rowKey);

  /**
   * Returns the row reader context for the given data object or null if null
   * exists
   * 
   * @param dataObject
   *          the data object
   * @return the row reader context for the given data object or null if null
   *         exists
   */
  public RowReader getRowReader(DataObject dataObject);

  /**
   * Adds the given row reader context mapping it to the given UUID.
   * 
   * @param uuid
   *          the UUID
   * @param rowContext
   *          the row reader context
   * @throws IllegalArgumentException
   *           if an existing row reader is already mapped for the given UUID
   */
  public void addRowReader(UUID uuid, RowReader rowContext) throws IllegalArgumentException;

  /**
   * Creates and adds a row reader based on the given data object and result
   * row.
   * 
   * @param dataObject
   *          the data object
   * @return the row reader
   * @throws IllegalArgumentException
   *           if an existing row reader is already mapped for the given data
   *           object UUID
   */
  public RowReader createRowReader(DataObject dataObject, CellValues resultRow)
      throws IllegalArgumentException;

  /**
   * Returns all row reader context values for this table context.
   * 
   * @return all row reader context values for this table context.
   */
  public List<RowReader> getAllRowReaders();

  /**
   * Frees resources associated with this reader and any component readers.
   */
  public void clear();

}
