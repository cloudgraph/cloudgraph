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

import java.io.IOException;
import java.util.List;

import commonj.sdo.DataObject;
import commonj.sdo.Type;

/**
 * Encapsulates one or more graph table writer components for federation across
 * multiple physical tables and/or physical table rows.
 * <p>
 * Acts as a container for one or more {@link TableWriter} elements
 * encapsulating a set of component table write operations for federation across
 * multiple tables, or a single table in the most simple (degenerate) case.
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.GraphTableWriter
 * @see org.cloudgraph.state.GraphTable
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface DistributedWriter extends DistributedGraphOperation {

  /**
   * Returns the table writer for the given configured table name.
   * 
   * @param tableName
   *          the name of the configured table.
   * @return the table writer for the given configured table name.
   */
  public TableWriter getTableWriter(String tableName);

  /**
   * Adds the given table writer to the container
   * 
   * @param writer
   *          the table writer.
   */
  public void addTableWriter(TableWriter writer);

  /**
   * Returns the count of table writers for this container.
   * 
   * @return the count of table writers for this container
   */
  public int getTableWriterCount();

  /**
   * Returns all table writers for the this container
   * 
   * @return all table writers for the this container
   */
  public List<TableWriter> getTableWriters();

  /**
   * Returns the table writer associated with the data graph root.
   * 
   * @return the table writer associated with the data graph root.
   */
  public TableWriter getRootTableWriter();

  /**
   * Sets the table writer associated with the data graph root.
   * 
   * @param writer
   *          the table writer
   */
  public void setRootTableWriter(TableWriter writer);

  /**
   * Returns the row writer associated with the given data object or null if no
   * row writer is associated.
   * 
   * @param dataObject
   *          the data object
   * @return the row writer associated with the given data object or null if no
   *         row writer is associated.
   */
  public RowWriter findRowWriter(DataObject dataObject);

  /**
   * Returns the row writer associated with the given data object
   * 
   * @param dataObject
   *          the data object
   * @return the row writer associated with the given data object
   * @throws IllegalArgumentException
   *           if the given data object is not associated with any row writer.
   */
  public RowWriter getRowWriter(DataObject dataObject);

  /**
   * Creates and returns a new row writer associated with the given data object.
   * 
   * @param dataObject
   *          the data object
   * @return a new row writer associated with the given data object.
   * @throws IllegalArgumentException
   *           if the given data object is already associated with a row writer.
   */
  public RowWriter createRowWriter(DataObject dataObject) throws IOException;

  /**
   * Returns a list of types associated with the given table operation.
   * 
   * @param reader
   *          the table operation
   * @return a list of types associated with the given table operation.
   */
  public List<Type> getTypes(TableWriter operation);
}
