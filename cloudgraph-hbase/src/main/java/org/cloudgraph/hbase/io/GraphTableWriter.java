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
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.hbase.connect.Connection;
import org.cloudgraph.hbase.connect.HBaseConnectionManager;
import org.cloudgraph.state.GraphTable;

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
 * @see org.cloudgraph.hbase.io.RowWriter
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class GraphTableWriter extends GraphTable implements TableWriter {
  private static Log log = LogFactory.getLog(GraphTableWriter.class);
  private Connection connection;
  private Table table;
  /** maps data object UUIDs to row writers */
  private Map<String, RowWriter> rowContextMap = new HashMap<String, RowWriter>();
  private DistributedGraphWriter distributedGraphWriter;

  public GraphTableWriter(TableConfig table) {
    super(table);
  }

  public GraphTableWriter(TableConfig table, DistributedGraphWriter distributedOperation) {
    super(table);
    this.distributedGraphWriter = distributedOperation;
  }

  @Override
  public DistributedWriter getDistributedWriter() {
    return this.distributedGraphWriter;
  }

  public void setDistributedWriter(DistributedGraphWriter distributedGraphWriter) {
    this.distributedGraphWriter = distributedGraphWriter;
  }

  @Override
  public Table getTable() {
    if (this.connection == null) {
      try {
        this.connection = HBaseConnectionManager.instance().getConnection();
        TableName tableName = TableName.valueOf(tableConfig.getQualifiedName());
        // Note: calling tableExists() using the admin HBase API is expensive
        // and is
        // showing up on CPU profiling results. Just call get table and catch :(
        // Unfortunately at least on windows client, below get operation does
        // not throw
        // anything and continues on.
        if (!this.connection.tableExists(tableName)) {
          HBaseConnectionManager.instance().createTable(this.connection, tableName);
          this.table = this.connection.getTable(tableName);
        } else {
          try {
            this.table = this.connection.getTable(tableName);
          } catch (TableNotFoundException | NamespaceNotFoundException e) {
            HBaseConnectionManager.instance().createTable(this.connection, tableName);
            this.table = this.connection.getTable(tableName);
          }
        }
      } catch (IOException e) {
        throw new OperationException(e);
      }
    }
    return this.table;
  }

  /**
   * Returns whether there is an active HBase table pooled connection for this
   * context.
   * 
   * @return whether there is an active HBase table pooled connection for this
   *         context.
   */
  public boolean hasConnection() {
    return this.connection != null;
  }

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
  public DistributedOperation getDistributedOperation() {
    return this.distributedGraphWriter;
  }

  // /**
  // * Sets the distributed context associated with this table
  // * operation context.
  // * @param distributedOperation the operation
  // */
  // @Override
  // public void setDistributedOperation(DistributedOperation
  // distributedOperation) {
  // this.distributedGraphWriter = distributedOperation;
  // }

  @Override
  public void close() throws IOException {
    try {
      // don't close table here, let the connection
      // deal with resources it controls
      if (this.connection != null)
        this.connection.close();
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    } finally {
      this.table = null;
      this.connection = null;
    }
  }

}
