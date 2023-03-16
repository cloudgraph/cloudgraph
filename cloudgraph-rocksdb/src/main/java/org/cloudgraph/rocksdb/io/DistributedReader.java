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

import java.util.List;

import javax.xml.namespace.QName;

import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataObject;
import commonj.sdo.Type;

/**
 * Encapsulates one or more graph table reader components for federation across
 * multiple physical tables and/or physical table rows.
 * <p>
 * Acts as a container for one or more {@link TableReader} elements
 * encapsulating a set of component table reader operations for federation
 * across multiple tables, or a single table in the most simple (degenerate)
 * case.
 * </p>
 * 
 * @see org.cloudgraph.rocksdb.io.GraphTableReader
 * @see org.cloudgraph.state.GraphTable
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface DistributedReader extends DistributedGraphOperation {

  /**
   * Returns the table reader for the given configured table name.
   * 
   * @param tableName
   *          the name of the configured table.
   * @return the table reader for the given configured table name.
   */
  public TableReader getTableReader(String tableName);

  /**
   * Returns the table reader for the given qualified type name.
   * 
   * @param qualifiedTypeName
   *          the qualified type name.
   * @return the table reader for the given qualified type name.
   */
  public TableReader getTableReader(QName qualifiedTypeName);

  /**
   * Adds the given table reader to the container
   * 
   * @param reader
   *          the table reader.
   */
  public void addTableReader(TableReader reader);

  /**
   * Returns the count of table readers for this container.
   * 
   * @return the count of table readers for this container
   */
  public int getTableReaderCount();

  /**
   * Returns all table readers for the this container
   * 
   * @return all table readers for the this container
   */
  public List<TableReader> getTableReaders();

  /**
   * Returns the table reader associated with the data graph root.
   * 
   * @return the table reader associated with the data graph root.
   */
  public TableReader getRootTableReader();

  /**
   * Sets the table reader associated with the data graph root.
   * 
   * @param reader
   *          the table reader
   */
  public void setRootTableReader(TableReader reader);

  /**
   * Returns the row reader associated with the given data object
   * 
   * @param dataObject
   *          the data object
   * @return the row reader associated with the given data object
   * @throws IllegalArgumentException
   *           if the given data object is not associated with any row reader.
   */
  public RowReader getRowReader(DataObject dataObject);

  public void mapRowReader(DataObject dataObject, RowReader rowReader);

  public void mapRowReader(long dataObjectSequence, PlasmaType type, RowReader rowReader);

  public void mapRowReader(byte[] rowKey, RowReader rowReader);

  /**
   * Returns a list of types associated with the given table operation.
   * 
   * @param reader
   *          the table operation
   * @return a list of types associated with the given table operation.
   */
  public List<Type> getTypes(TableReader operation);

  /**
   * Frees resources associated with this reader and any component readers.
   */
  public void clear();
}
