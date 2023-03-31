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
package org.cloudgraph.core.service;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.core.Connection;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.io.TableWriter;
import org.cloudgraph.core.mutation.GraphMutationCollector;
import org.cloudgraph.core.mutation.GraphMutationWriter;
import org.cloudgraph.core.mutation.Mutations;
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
 * {org.cloudgraph.core.key.GraphRowKeyFactory factory} is used to create a new
 * composite HBase row key. The row key generation is driven by a configured
 * CloudGraph row key {@link org.cloudgraph.store.mapping.RowKeyModel model} for
 * a specific HTable {@link org.cloudgraph.store.mapping.Table configuration}. A
 * minimal set of {@link org.cloudgraph.state.SequenceGenerator state}
 * information is persisted with each new data graph.
 * </p>
 * <p>
 * For data graphs with any other combination of changes, e.g. data object
 * modifications, deletes, etc... an existing HBase row key is fetched using an
 * HBase <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Get.html"
 * target="#">Get</a> operation.
 * </p>
 * 
 * @see org.cloudgraph.core.io.DistributedWriter
 * @see org.cloudgraph.core.key.GraphRowKeyFactory
 * @see org.cloudgraph.core.key.GraphColumnKeyFactory
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
    Connection connection = this.context.getConnectionManager().getConnection();
    try {
      // FIXME: if an exception happens here we don't have table writers to
      // close
      // as required by the 1.0.0 HBase client API. Will cause resource bleed
      Map<TableWriter, Map<String, Mutations>> mutations = collectChanges(dataGraph, connection);
      if (log.isDebugEnabled())
        log.debug("collected " + mutations.keySet().size() + " table changes");
      TableWriter[] tableWriters = new TableWriter[mutations.keySet().size()];
      mutations.keySet().toArray(tableWriters);
      GraphMutationWriter writer = new GraphMutationWriter(this.context);
      try {
        writer.writeChanges(tableWriters, mutations, this.snapshotMap, this.username);
      } finally {
        for (TableWriter tableWriter : tableWriters)
          tableWriter.close();
      }
      return snapshotMap;
    } catch (IOException | IllegalAccessException e) {
      throw new DataAccessException(e);
    } finally {
      try {
        connection.close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
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

    Connection connection = this.context.getConnectionManager().getConnection();
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
          Map<TableWriter, Map<String, Mutations>> mutations = collectChanges(dataGraph, connection);
          if (log.isDebugEnabled())
            log.debug("graph " + i + " of " + dataGraphs.length + " - collected "
                + mutations.keySet().size() + " table changes");
          tableWriters = new TableWriter[mutations.keySet().size()];
          mutations.keySet().toArray(tableWriters);
          GraphMutationWriter writer = new GraphMutationWriter(this.context);

          writer.writeChanges(tableWriters, mutations, this.snapshotMap, this.username);
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
    } finally {
      try {
        connection.close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }
  }

}
