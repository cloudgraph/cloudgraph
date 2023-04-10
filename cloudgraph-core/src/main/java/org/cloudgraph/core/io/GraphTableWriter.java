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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.BufferedMutator;
import org.cloudgraph.core.client.Table;
import org.cloudgraph.core.client.TableName;
import org.cloudgraph.state.GraphTable;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

/**
 * The operational, configuration and other state information required for write
 * operations on a single graph table.
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
public class GraphTableWriter extends GraphTable implements TableWriter {
  private static Log log = LogFactory.getLog(GraphTableWriter.class);
  private Table table;
  private BufferedMutator bufferedMutator;
  /** maps data object UUIDs to row writers */
  private Map<String, RowWriter> rowContextMap = new HashMap<String, RowWriter>();
  private DistributedWriter distributedGraphWriter;
  public boolean hasConcurentRows = false;
  protected ServiceContext serviceContext;

  public GraphTableWriter(TableMapping table, ServiceContext serviceContext) {
    super(table, serviceContext.getStoreMapping());
    this.serviceContext = serviceContext;
  }

  public GraphTableWriter(TableMapping table, DistributedGraphWriter distributedOperation,
      ServiceContext serviceContext) {
    super(table, serviceContext.getStoreMapping());
    this.distributedGraphWriter = distributedOperation;
    this.serviceContext = serviceContext;
  }

  @Override
  public DistributedWriter getDistributedWriter() {
    return this.distributedGraphWriter;
  }

  @Override
  public void setDistributedWriter(DistributedWriter distributedGraphWriter) {
    this.distributedGraphWriter = distributedGraphWriter;
  }

  /**
   * 
   * Returns the qualified logical table name associated with this reader.
   * 
   * @return the qualified logical table name associated with this reader.
   */
  @Override
  public String getQualifiedLogicalTableName() {
    return this.getTableConfig().getQualifiedLogicalName();
  }

  /**
   * 
   * Returns the qualified physical table name associated with this reader.
   * 
   * @return the qualified physical table name associated with this reader.
   */
  @Override
  public String getPhysicalTableName() {
    return this.getTableConfig().getPhysicalName();
  }

  /**
   * 
   * Returns the qualified physical table namespace associated with this reader.
   * 
   * @return the qualified physical table namespace associated with this reader.
   */
  @Override
  public String getQualifiedPhysicalTableNamespace() {
    return this.getTableConfig().getQualifiedPhysicalNamespace();
  }

  @Override
  public Table getTable() {
    try {
      TableName tableName = serviceContext.getClientFactory().createTableName(
          getQualifiedPhysicalTableNamespace(), getPhysicalTableName());
      // Note: calling tableExists() using the admin HBase API is expensive
      // and is
      // showing up on CPU profiling results. Just call get table and catch :(
      // Unfortunately at least on windows client, below get operation does
      // not throw
      // anything and continues on.
      if (!distributedGraphWriter.getConnection().tableExists(tableName)) {
        serviceContext.getConnectionManager().createTable(distributedGraphWriter.getConnection(),
            tableName, this.mappingContext);
        this.table = distributedGraphWriter.getConnection().getTable(tableName);
      } else {
        try {
          this.table = distributedGraphWriter.getConnection().getTable(tableName);
        } catch (IOException e) {
          serviceContext.getConnectionManager().createTable(distributedGraphWriter.getConnection(),
              tableName, this.mappingContext);
          this.table = distributedGraphWriter.getConnection().getTable(tableName);
        }
      }
    } catch (IOException e) {
      throw new OperationException(e);
    }
    return this.table;
  }

  // @Override
  // public BufferedMutator getBufferedMutator() {
  // try {
  // TableName tableName =
  // TableName.valueOf(getQualifiedPhysicalTableNamespace(),
  // getQualifiedPhysicalTableName());
  // // Note: calling tableExists() using the admin HBase API is expensive
  // // and is
  // // showing up on CPU profiling results. Just call get table and catch :(
  // // Unfortunately at least on windows client, below get operation does
  // // not throw
  // // anything and continues on.
  // if (!distributedGraphWriter.getConnection().tableExists(tableName)) {
  // HBaseConnectionManager.instance().createTable(distributedGraphWriter.getConnection(),
  // tableName, this.mappingContext);
  // this.bufferedMutator =
  // distributedGraphWriter.getConnection().getBufferedMutator(tableName);
  // } else {
  // try {
  // this.bufferedMutator =
  // distributedGraphWriter.getConnection().getBufferedMutator(
  // tableName);
  // } catch (IOException e) {
  // HBaseConnectionManager.instance().createTable(distributedGraphWriter.getConnection(),
  // tableName, this.mappingContext);
  // this.bufferedMutator =
  // distributedGraphWriter.getConnection().getBufferedMutator(
  // tableName);
  // }
  // }
  // } catch (IOException e) {
  // throw new OperationException(e);
  // }
  // return this.bufferedMutator;
  // }

  @Override
  public RowWriter getRowWriter(UUID uuid) {
    return rowContextMap.get(uuid.toString());
  }

  @Override
  public void addRowWriter(UUID uuid, RowWriter rowContext) {
    rowContextMap.put(uuid.toString(), rowContext);
  }

  @Override
  public List<RowWriter> getAllRowWriters() {
    List<RowWriter> result = new ArrayList<RowWriter>();
    result.addAll(rowContextMap.values());
    return result;
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
    return this.distributedGraphWriter;
  }

  @Override
  public void close() {
    this.table = null;
    this.bufferedMutator = null;
  }

  @Override
  public boolean hasConcurrentRows() {
    return this.hasConcurentRows;
  }

  @Override
  public void setHasConcurrentRows(boolean value) {
    this.hasConcurentRows = value;
  }

}
