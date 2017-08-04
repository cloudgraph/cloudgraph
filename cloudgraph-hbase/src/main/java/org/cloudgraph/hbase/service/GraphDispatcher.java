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
package org.cloudgraph.hbase.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.io.TableWriter;
import org.cloudgraph.hbase.mutation.GraphMutationCollector;
import org.cloudgraph.store.service.DuplicateRowException;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.access.DataGraphDispatcher;
import org.plasma.sdo.core.SnapshotMap;

import commonj.sdo.DataGraph;

/**
 * Propagates changes to a {@link commonj.sdo.DataGraph data graph} including
 * any number of creates (inserts), modifications (updates) and deletes across
 * one or more HBase table rows.
 * <p>
 * For new (created) data graphs, a row key
 * {org.cloudgraph.hbase.key.HBaseRowKeyFactory factory} is used to create a new
 * composite HBase row key. The row key generation is driven by a configured
 * CloudGraph row key {@link org.cloudgraph.config.RowKeyModel model} for a
 * specific HTable {@link org.cloudgraph.config.Table configuration}. A minimal
 * set of {@link org.cloudgraph.state.SequenceGenerator state} information is
 * persisted with each new data graph.
 * </p>
 * <p>
 * For data graphs with any other combination of changes, e.g. data object
 * modifications, deletes, etc... an existing HBase row key is fetched using an
 * HBase <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Get.html"
 * target="#">Get</a> operation.
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.DistributedWriter
 * @see org.cloudgraph.store.key.GraphRowKeyFactory
 * @see org.cloudgraph.store.key.GraphColumnKeyFactory
 * @see org.cloudgraph.state.SequenceGenerator
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class GraphDispatcher extends GraphMutationCollector implements DataGraphDispatcher {
  private static Log log = LogFactory.getLog(GraphDispatcher.class);

  public GraphDispatcher(ServiceContext context, SnapshotMap snapshotMap, String username) {
    super(context, snapshotMap, username);
  }

  public void close() {
    this.context.close();
  }

  /**
   * Propagates changes to the given <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/sdo/PlasmaDataGraph.html"
   * target="#">data graph</a> including any number of creates (inserts),
   * modifications (updates) and deletes to a single or multiple HBase tables
   * and table rows.
   * 
   * @return a map of internally managed concurrency property values and data
   *         store generated keys.
   * @throws DuplicateRowException
   *           if for a new data graph, the generated row key already exists in
   *           the HBase table configured .
   */
  public SnapshotMap commit(DataGraph dataGraph) {
    if (username == null || username.length() == 0)
      throw new IllegalArgumentException("expected username param not, '"
          + String.valueOf(username) + "'");
    else if (log.isDebugEnabled()) {
      log.debug("current user is '" + username + "'");
    }

    try {
      // FIXME: if an exception happens here we don't have table writers to
      // close
      // as required by the 1.0.0 HBase client API. Will cause resource bleed
      Map<TableWriter, List<Row>> mutations = collectChanges(dataGraph);
      if (log.isDebugEnabled())
        log.debug("collected " + mutations.keySet().size() + " table changes");
      TableWriter[] tableWriters = new TableWriter[mutations.keySet().size()];
      mutations.keySet().toArray(tableWriters);
      try {
        this.writeChanges(tableWriters, mutations, this.username);
      } finally {
        for (TableWriter tableWriter : tableWriters)
          tableWriter.close();
      }
      return snapshotMap;
    } catch (IOException | IllegalAccessException e) {
      throw new DataAccessException(e);
    }
  }

  /**
   * Propagates changes to the given array of <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/sdo/PlasmaDataGraph.html"
   * target="#">data graphs</a> including any number of creates (inserts),
   * modifications (updates) and deletes to a single or multiple HBase tables
   * and table rows. The given graphs may be heterogeneous, with different root
   * data objects any 'shape' or depth.
   * 
   * @return a map of internally managed concurrency property values and data
   *         store generated keys.
   * @throws DuplicateRowException
   *           if for a new data graph, the generated row key already exists in
   *           the HBase table configured .
   */
  public SnapshotMap commit(DataGraph[] dataGraphs) {
    if (username == null || username.length() == 0)
      throw new IllegalArgumentException("expected username param not, '"
          + String.valueOf(username) + "'");
    else if (log.isDebugEnabled()) {
      log.debug("current user is '" + username + "'");
    }

    try {
      // Note: multiple data graphs may share one or more data objects and here
      // we force
      // a write for each graph so the state will be saved and re-read from data
      // store
      // such that any common data object(s) state will be current.
      int i = 1;
      for (DataGraph dataGraph : dataGraphs) {
        if (log.isDebugEnabled())
          log.debug("collecting changes graph " + i + " of " + dataGraphs.length + "");
        TableWriter[] tableWriters = null;
        try {
          Map<TableWriter, List<Row>> mutations = collectChanges(dataGraph);
          if (log.isDebugEnabled())
            log.debug("graph " + i + " of " + dataGraphs.length + " - collected "
                + mutations.keySet().size() + " table changes");
          tableWriters = new TableWriter[mutations.keySet().size()];
          mutations.keySet().toArray(tableWriters);
          this.writeChanges(tableWriters, mutations, this.username);
        } finally {
          if (tableWriters != null)
            for (TableWriter tableWriter : tableWriters)
              tableWriter.close();
          i++;
        }
      }
      return snapshotMap;
    } catch (IOException | IllegalAccessException | GraphServiceException e) {
      throw new DataAccessException(e);
    }
  }

  private void writeChanges(TableWriter[] tableWriters, Map<TableWriter, List<Row>> mutations,
      String jobName) throws IOException {
    for (TableWriter tableWriter : tableWriters) {
      List<Row> tableMutations = mutations.get(tableWriter);
      if (log.isDebugEnabled())
        log.debug("commiting " + tableMutations.size() + " mutations to table: "
            + tableWriter.getTableConfig().getName());
      if (log.isDebugEnabled()) {
        for (Row row : tableMutations) {
          log.debug("commiting " + row.getClass().getSimpleName() + " mutation to table: "
              + tableWriter.getTableConfig().getName());
          debugRowValues(row);
        }
      }
      Object[] results = new Object[tableMutations.size()];
      try {
        tableWriter.getTable().batch(tableMutations, results);
      } catch (InterruptedException e) {
        throw new GraphServiceException(e);
      }
      for (int i = 0; i < results.length; i++) {
        if (results[i] == null) {
          log.error("batch action (" + i + ") for job '" + jobName + "' failed with null result");
        } else {
          if (log.isDebugEnabled())
            log.debug("batch action (" + i + ") for job '" + jobName + "' succeeded with "
                + String.valueOf(results[i]) + " result");
        }
      }
      // tableWriter.getTable().flushCommits();
      // FIXME: find what happened to flush
    }
  }

  private void debugRowValues(Row row) {
    if (row instanceof Mutation) {
      Mutation mutation = (Mutation) row;
      NavigableMap<byte[], List<Cell>> map = mutation.getFamilyCellMap();
      StringBuilder buf = new StringBuilder();
      Iterator<byte[]> iter = map.keySet().iterator();
      buf.append("[");
      int i = 0;
      while (iter.hasNext()) {
        if (i > 0)
          buf.append(", ");
        byte[] family = iter.next();
        List<Cell> list = map.get(family);
        for (Cell cell : list) {
          buf.append(Bytes.toString(family));
          buf.append(":");
          byte[] qual = CellUtil.cloneQualifier(cell);
          buf.append(Bytes.toString(qual));
          buf.append("=");
          byte[] value = CellUtil.cloneValue(cell);
          buf.append(Bytes.toString(value));
        }
      }
      buf.append("]");
      log.debug("values: " + buf.toString());
    }
  }
}
