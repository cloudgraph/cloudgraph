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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rocksdb.connect.Connection;
import org.cloudgraph.state.GraphRow;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataObject;
import commonj.sdo.Type;

/**
 * Encapsulates one or more graph table reader components for federation across
 * multiple physical tables and/or physical table rows. Maps physical configured
 * table names to respective table readers. In most usage scenarios, a "root"
 * table reader is typically added initially, then other reader are
 * incrementally added as association target types are detected and found
 * configured as graph roots within another distinct table context.
 * <p>
 * Acts as a container for one or more {@link TableReader} elements
 * encapsulating a set of component table read operations for distributed
 * assembly across multiple tables, or a single table in the most simple
 * (degenerate) case.
 * </p>
 * 
 * @see org.cloudgraph.rocksdb.io.GraphTableReader
 * @see org.cloudgraph.state.GraphTable
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class DistributedGraphReader implements DistributedReader {

  private static Log log = LogFactory.getLog(DistributedGraphReader.class);

  private TableReader rootReader;
  /** maps table names to table readers */
  private Map<String, TableReader> tableReaderMap = new HashMap<String, TableReader>();
  /** maps qualified graph-root type names to table readers */
  private Map<QName, TableReader> typeTableReaderMap = new HashMap<QName, TableReader>();
  /** maps table readers to graph-root types */
  private Map<TableReader, List<Type>> tableReaderTypeMap = new HashMap<TableReader, List<Type>>();

  // maps data objects to row readers
  private Map<Integer, RowReader> rowReaderMap = new HashMap<Integer, RowReader>();

  private Connection connection;

  private StoreMappingContext mappingContext;

  @SuppressWarnings("unused")
  private DistributedGraphReader() {
  }

  public DistributedGraphReader(Type rootType, List<Type> types, Connection connection,
      StoreMappingContext mappingContext) {
    PlasmaType root = (PlasmaType) rootType;

    this.mappingContext = mappingContext;
    TableMapping rootTable = StoreMapping.getInstance().getTable(root.getQualifiedName(),
        this.mappingContext);
    this.connection = connection;

    TableReader tableReader = new GraphTableReader(rootTable, this, this.mappingContext);
    this.tableReaderMap.put(tableReader.getTableConfig().getQualifiedLogicalName(), tableReader);

    this.rootReader = tableReader;
    this.typeTableReaderMap.put(((PlasmaType) rootType).getQualifiedName(), this.rootReader);
    List<Type> list = new ArrayList<Type>();
    this.tableReaderTypeMap.put(this.rootReader, list);
    list.add(rootType);

    for (Type t : types) {
      PlasmaType type = (PlasmaType) t;
      TableMapping table = StoreMapping.getInstance().findTable(type.getQualifiedName(),
          this.mappingContext);
      if (table == null)
        continue; // not a graph root

      tableReader = this.tableReaderMap.get(table.getQualifiedLogicalName());
      if (tableReader == null) {
        // create a new table reader if not added already, e.g.
        // as root above or from a graph root type
        // mapped to a table we have seen here
        tableReader = new GraphTableReader(table, this, this.mappingContext);
        this.tableReaderMap
            .put(tableReader.getTableConfig().getQualifiedLogicalName(), tableReader);
      }

      // always map root types
      this.typeTableReaderMap.put(type.getQualifiedName(), tableReader);

      list = this.tableReaderTypeMap.get((TableOperation) tableReader);
      if (list == null) {
        list = new ArrayList<Type>();
        this.tableReaderTypeMap.put(tableReader, list);
      }
      if (!list.contains(type))
        list.add(type);
    }
  }

  /**
   * Returns the table reader for the given configured table name, or null of
   * not exists.
   * 
   * @param tableName
   *          the name of the configured table.
   * @return the table reader for the given configured table name, or null of
   *         not exists.
   */
  @Override
  public TableReader getTableReader(String tableName) {
    return this.tableReaderMap.get(tableName);
  }

  /**
   * Returns the table reader for the given qualified type name, or null if not
   * exists.
   * 
   * @param qualifiedTypeName
   *          the qualified type name.
   * @return the table reader for the given qualified type name, or null if not
   *         exists.
   */
  public TableReader getTableReader(QName qualifiedTypeName) {
    return this.typeTableReaderMap.get(qualifiedTypeName);
  }

  /**
   * Adds the given table reader to the container
   * 
   * @param reader
   *          the table reader.
   */
  @Override
  public void addTableReader(TableReader reader) {
    String name = reader.getTableConfig().getQualifiedLogicalName();
    if (this.tableReaderMap.get(name) != null)
      throw new OperationException("table reader for '" + name + "' already exists");
    this.tableReaderMap.put(name, reader);
  }

  /**
   * Returns the count of table readers for this container.
   * 
   * @return the count of table readers for this container
   */
  @Override
  public int getTableReaderCount() {
    return this.tableReaderMap.size();
  }

  /**
   * Returns all table readers for the this container
   * 
   * @return all table readers for the this container
   */
  public List<TableReader> getTableReaders() {
    List<TableReader> result = new ArrayList<TableReader>();
    result.addAll(this.tableReaderMap.values());
    return result;
  }

  /**
   * Returns the table reader associated with the data graph root.
   * 
   * @return the table reader associated with the data graph root.
   */
  public TableReader getRootTableReader() {
    return this.rootReader;
  }

  /**
   * Sets the table reader associated with the data graph root.
   * 
   * @param reader
   *          the table reader
   */
  public void setRootTableReader(TableReader reader) {
    this.rootReader = reader;
    this.tableReaderMap.put(rootReader.getTableConfig().getQualifiedLogicalName(), rootReader);
  }

  /**
   * Returns the row reader associated with the given data object
   * 
   * @param dataObject
   *          the data object
   * @return the row reader associated with the given data object
   * @throws IllegalArgumentException
   *           if the given data object is not associated with any row reader.
   */
  public RowReader getRowReader(DataObject dataObject) {
    RowReader result = rowReaderMap.get(dataObject.hashCode());
    if (result == null)
      throw new IllegalArgumentException("the given data object of type " + dataObject.getType()
          + " is not associated with any row reader");
    return result;
  }

  public void mapRowReader(DataObject dataObject, RowReader rowReader) {
    int key = dataObject.hashCode();
    RowReader existing = this.rowReaderMap.get(key);
    if (existing != null) {
      if (log.isDebugEnabled())
        log.debug("the given data object of type " + dataObject.getType()
            + " is already associated with a row reader - ignoring");
      return;
    }

    rowReaderMap.put(key, rowReader);
  }

  @Override
  public void mapRowReader(long dataObjectSequence, PlasmaType type, RowReader rowReader) {

    int key = GraphRow.getHashCode(dataObjectSequence, type);
    RowReader existing = this.rowReaderMap.get(key);
    if (existing != null) {
      if (log.isDebugEnabled())
        log.debug("the given data object with sequence " + dataObjectSequence + " of type " + type
            + " is already associated with a row reader - ignoring");
      return;
    }

    rowReaderMap.put(key, rowReader);
  }

  @Override
  public void mapRowReader(byte[] rowKey, RowReader rowReader) {
    int key = Arrays.hashCode(rowKey);
    RowReader existing = this.rowReaderMap.get(key);
    if (existing != null) {
      if (log.isDebugEnabled())
        log.debug("the given row key " + rowKey
            + " is already associated with a row reader - ignoring");
      return;
    }

    rowReaderMap.put(key, rowReader);
  }

  /**
   * Returns a list of types associated with the given table reader.
   * 
   * @param reader
   *          the table reader
   * @return a list of types associated with the given table reader.
   */
  @Override
  public List<Type> getTypes(TableReader operation) {
    return this.tableReaderTypeMap.get(operation);
  }

  /**
   * Returns true if only one table operation exists with only one associated
   * (root) type for this operation.
   * 
   * @return true if only one table operation exists with only one associated
   *         (root) type for this operation.
   */
  public boolean hasSingleRootType() {
    if (this.getTableReaderCount() == 1 && this.getTypes(this.rootReader).size() == 1) {
      return true;
    } else
      return false;
  }

  /**
   * Frees resources associated with this reader and any component readers.
   */
  public void clear() {
    this.rowReaderMap.clear();
    // table readers are created based entirely on metadata
    // i.e. selected types for a query
    for (TableReader tableReader : tableReaderMap.values())
      tableReader.clear();
  }

  @Override
  public Connection getConnection() {
    return this.connection;
  }

  @Override
  public void close() {
    for (TableReader reader : this.getTableReaders()) {
      reader.close();
    }
    // don't close the connection here, we don't own it
    this.connection = null;
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return this.mappingContext;
  }

}