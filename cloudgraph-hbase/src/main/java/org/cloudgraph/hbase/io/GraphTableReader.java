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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.hbase.connect.HBaseConnectionManager;
import org.cloudgraph.state.GraphTable;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
//import org.cloudgraph.state.UUID;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.core.CoreDataObject;

import commonj.sdo.DataObject;

/**
 * The operational, configuration and other state information required for read
 * operations on a single graph table.
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
public class GraphTableReader extends GraphTable implements TableReader {
  private static Log log = LogFactory.getLog(GraphTableReader.class);

  private Table table;
  /** maps data object UUIDs strings and row key strings to row readers */
  private Map<Object, RowReader> rowReaderMap = new HashMap<>();
  private DistributedGraphOperation distributedOperation;

  public GraphTableReader(TableMapping table, DistributedGraphOperation distributedOperation,
      StoreMappingContext mappingContext) {
    super(table, mappingContext);
    this.distributedOperation = distributedOperation;
  }

  /**
   * 
   * Returns the qualified table name associated with this reader.
   * 
   * @return the qualified table name associated with this reader.
   */
  public String getTableName() {
    return this.getTableConfig().getQualifiedName();
  }

  @Override
  public Table getTable() {
    try {
      TableName tableName = TableName.valueOf(tableConfig.getQualifiedName());
      // Note: calling tableExists() using the admin HBase API is expensive
      // and is
      // showing up on CPU profiling results. Just call get table and catch :(
      if (!this.distributedOperation.getConnection().tableExists(tableName)) {
        HBaseConnectionManager.instance().createTable(this.distributedOperation.getConnection(),
            tableName, this.mappingContext);
        this.table = this.distributedOperation.getConnection().getTable(tableName);
      } else {
        try {
          this.table = this.distributedOperation.getConnection().getTable(tableName);
        } catch (TableNotFoundException | NamespaceNotFoundException e) {
          HBaseConnectionManager.instance().createTable(this.distributedOperation.getConnection(),
              tableName, this.mappingContext);
          this.table = this.distributedOperation.getConnection().getTable(tableName);
        }
      }
    } catch (IOException e) {
      throw new OperationException(e);
    }
    return this.table;
  }

  /**
   * Returns the row reader context for the given data object or null if null
   * exists
   * 
   * @param dataObject
   *          the data object
   * @return the row reader context for the given data object or null if null
   *         exists
   */
  @Override
  public RowReader getRowReader(DataObject dataObject) {
    String uuid = ((PlasmaDataObject) dataObject).getUUIDAsString();
    return rowReaderMap.get(uuid);
  }

  /**
   * Returns the row reader context for the given row key or null if null exists
   * 
   * @param rowKey
   *          the row key bytes
   * @return the row reader context for the given row key or null if null exists
   */
  @Override
  public RowReader getRowReader(byte[] rowKey) {
    return rowReaderMap.get(Bytes.toString(rowKey));
  }

  @Override
  public RowReader getRowReader(String rowKey) {
    return rowReaderMap.get(rowKey);
  }

  /**
   * Returns the row reader context for the given UUID
   * 
   * @param uuid
   *          the UUID
   * @return the row reader context for the given UUID
   */
  @Override
  public RowReader getRowReader(UUID uuid) {
    return rowReaderMap.get(uuid.toString());
  }

  /**
   * Adds the given row reader context mapping it to the given UUID
   * 
   * @param uuid
   *          the UUID
   * @param rowReader
   *          the row reader context
   * @throws IllegalArgumentException
   *           if an existing row reader is already mapped for the given data
   *           object UUID
   */
  @Override
  public void addRowReader(UUID uuid, RowReader rowReader) throws IllegalArgumentException {
    if (rowReaderMap.get(uuid) != null)
      throw new IllegalArgumentException("given UUID already mapped to a row reader, " + uuid);
    rowReaderMap.put(uuid, rowReader);
  }

  @Override
  public void addRowReader(String rowKey, RowReader rowReader) throws IllegalArgumentException {
    if (rowReaderMap.get(rowKey) != null)
      throw new IllegalArgumentException("given row key already mapped to a row reader, " + rowKey);
    rowReaderMap.put(rowKey, rowReader);
  }

  /**
   * Returns all row reader context values for this table context.
   * 
   * @return all row reader context values for this table context.
   */
  @Override
  public List<RowReader> getAllRowReaders() {
    List<RowReader> result = new ArrayList<RowReader>();
    result.addAll(rowReaderMap.values());
    return result;
  }

  /**
   * Creates and adds a row reader based on the given data object and result row
   * mapping it by UUID string and row key string.
   * 
   * @param dataObject
   *          the data object
   * @return the row reader
   * @throws IllegalArgumentException
   *           if an existing row reader is already mapped for the given data
   *           object UUID
   */
  @Override
  public RowReader createRowReader(DataObject dataObject, CellValues resultRow)
      throws IllegalArgumentException {

    byte[] rowKey = resultRow.getRowKeyAsBytes();
    String keyString = Bytes.toString(rowKey);
    UUID uuid = ((PlasmaDataObject) dataObject).getUUID();
    if (this.rowReaderMap.containsKey(uuid))
      throw new IllegalArgumentException("given UUID already mapped to a row reader, " + uuid);
    if (this.rowReaderMap.containsKey(keyString))
      throw new IllegalArgumentException(
          "existing row reader is already mapped for the given row key, " + keyString);
    GraphRowReader rowReader = new GraphRowReader(rowKey, resultRow, dataObject, this,
        this.mappingContext);
    this.addRowReader(uuid, rowReader);
    this.addRowReader(keyString, rowReader);

    // set the row key so we can look it up on
    // modify and delete ops
    CoreDataObject coreObject = (CoreDataObject) dataObject;
    coreObject.getValueObject().put(CloudGraphConstants.ROW_KEY, rowReader.getRowKey());

    return rowReader;
  }

  /**
   * Returns the distributed context associated with this table operation
   * context.
   * 
   * @return the distributed context associated with this table operation
   *         context.
   */
  @Override
  public DistributedGraphOperation getDistributedOperation() {
    return this.distributedOperation;
  }

  /**
   * Frees resources associated with this reader and any component readers.
   */
  public void clear() {
    this.rowReaderMap.clear();
  }

  @Override
  public void close() {
    this.table = null;
  }

}
