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
package org.cloudgraph.hbase.graph;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.common.concurrent.SubgraphTask;
import org.cloudgraph.config.ThreadPoolConfigProps;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.hbase.io.CellValues;
import org.cloudgraph.hbase.io.DistributedReader;
import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.io.OperationException;
import org.cloudgraph.hbase.io.RowReader;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.service.GraphServiceException;
//import org.cloudgraph.state.GraphState.Edge;
import org.plasma.query.collector.Selection;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

/**
 * A concurrent assembly task which assembles a sub-graph starting with a given
 * "sub root" based on the given <a target="#"
 * href="http://plasma-sdo.org/org/plasma/query/collector/Selection.html"
 * >"selection graph"</a>. Processing proceeds as a breadth-first traversal and
 * additional tasks are dynamically spawned based on thread availability within
 * a shared <a href=
 * "https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor.html"
 * >thread pool</a>. If thread availability is exhausted, processing proceeds
 * within the current thread.
 * 
 * @see DistributedReader
 * @see RowReader
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
// package protection
class ParallelSubgraphTask extends DefaultSubgraphTask implements SubgraphTask {
  private static Log log = LogFactory.getLog(ParallelSubgraphTask.class);

  /**
   * Constructor.
   * 
   * @param subroot
   *          the graph sub root
   * @param selection
   *          the graph selection
   * @param snapshotDate
   *          the snapshot date
   * @param distributedReader
   *          the distributed reader
   * @param source
   *          the source data object representing the source edge
   * @param sourceProperty
   *          the source property representing the source edge
   * @param rowReader
   *          the row reader
   * @param level
   *          the traversal level
   * @param taskSequence
   *          the task sequence
   * @param executorService
   *          the thread pool reference
   */
  public ParallelSubgraphTask(PlasmaDataObject subroot, long subrootSequence, Selection selection,
      Timestamp snapshotDate, DistributedReader distributedReader, EdgeReader collection,
      PlasmaDataObject source, PlasmaProperty sourceProperty, RowReader rowReader, int level,
      int taskSequence, ThreadPoolExecutor executorService, ThreadPoolConfigProps config) {
    super(subroot, subrootSequence, selection, snapshotDate, distributedReader, collection, source,
        sourceProperty, rowReader, level, taskSequence, executorService, config);
  }

  /**
   * Factory method creating a new task.
   * 
   * @param subroot
   *          the graph sub root
   * @param selection
   *          the graph selection
   * @param snapshotDate
   *          the snapshot date
   * @param distributedReader
   *          the distributed reader
   * @param source
   *          the source data object representing the source edge
   * @param sourceProperty
   *          the source property representing the source edge
   * @param rowReader
   *          the row reader
   * @param level
   *          the traversal level
   * @param sequence
   *          the task sequence
   * @param executorService
   *          the thread pool reference
   * @return the task
   */
  @Override
  protected SubgraphTask newTask(PlasmaDataObject subroot, long subrootSequence,
      Selection selection, Timestamp snapshotDate, DistributedReader distributedReader,
      EdgeReader collection, PlasmaDataObject source, PlasmaProperty sourceProperty,
      RowReader rowReader, int level, int sequence, ThreadPoolExecutor executorService,
      ThreadPoolConfigProps config) {
    return new ParallelSubgraphTask(subroot, subrootSequence, selection, snapshotDate,
        distributedReader, collection, source, sourceProperty, rowReader, level, sequence,
        executorService, config);
  }

  @Override
  protected void assemble(PlasmaDataObject target, long targetSequence,
      EdgeReader sourceCollection, PlasmaDataObject source, PlasmaProperty sourceProperty,
      RowReader rowReader, int level) throws IOException {

    Set<Property> props = this.getProperties(target, source, sourceProperty, level);
    if (props.size() == 0)
      return;
    if (log.isDebugEnabled())
      log.debug("assembling(" + level + "): " + target.toString() + ": " + props.toString());

    // synchronize on row-reader here rather than target because row-reader
    // uses shared column key factory
    synchronized (rowReader) {
      assembleData(target, targetSequence, props, rowReader);
    }

    TableReader tableReader = rowReader.getTableReader();
    TableConfig tableConfig = tableReader.getTableConfig();

    traversals.clear();

    // reference props
    for (Property p : props) {
      PlasmaProperty prop = (PlasmaProperty) p;
      if (prop.getType().isDataType())
        continue;

      EdgeReader edgeReader = null;
      if (rowReader.edgeExists((PlasmaType) target.getType(), prop, targetSequence)) {
        edgeReader = rowReader.getEdgeReader((PlasmaType) target.getType(), prop, targetSequence);
      } else
        continue; // edge not found in data

      if (!edgeReader.isExternal()) {
        assembleEdges(target, targetSequence, prop, edgeReader, rowReader, tableReader, rowReader,
            level);
      } else {
        TableReader externalTableReader = distributedReader.getTableReader(edgeReader.getTable());
        if (externalTableReader == null)
          throw new OperationException("no table reader found for type, "
              + edgeReader.getBaseType());
        assembleExternalEdges(target, targetSequence, prop, edgeReader, rowReader,
            externalTableReader, level);
      }
    }

    traverse(level);
  }

  protected void assembleEdges(PlasmaDataObject target, long targetSequence, PlasmaProperty prop,
      EdgeReader collection, RowReader rowReader, TableReader childTableReader,
      RowReader childRowReader, int level) throws IOException {
    for (long childSequence : collection.getSequences()) {
      PlasmaType subType = collection.getSubType();
      if (subType == null)
        subType = collection.getBaseType();

      if (childRowReader.contains(childSequence, subType)) {
        // we've seen this child before so his data is complete, just
        // link
        PlasmaDataObject existingChild = (PlasmaDataObject) childRowReader.getDataObject(
            childSequence, collection.getBaseType());
        synchronized (existingChild) {
          synchronized (target) {
            link(existingChild, target, prop);
          }
        }
        continue;
      }

      if (log.isDebugEnabled())
        log.debug("local edge: " + target.getType().getURI() + "#" + target.getType().getName()
            + "->" + prop.getName() + " (" + childSequence + ")");

      synchronized (this.distributedReader) {
        this.distributedReader.mapRowReader(childSequence, subType, childRowReader);
      }

      if (log.isDebugEnabled())
        log.debug("traverse: (" + prop.getName() + ") ");

      PlasmaDataObject child = null;
      synchronized (target) {
        child = createChild(childSequence, collection, target, prop, childRowReader, this.graph);
      }

      synchronized (childRowReader) {
        childRowReader.addDataObject(child);
      }

      // indicate a non-concurrent traversal given this
      // is not a slice assembler no fetch occurring for internal edges
      traversals.add(new Traversal(child, childSequence, collection, target, prop, childRowReader,
          false, // indicate a
          // non-concurrent
          // traversal
          level + 1));
    }
  }

  /**
   * Assembles a given set of edges where the target is a different row, within
   * this table or another. Since we are assembling a graph, and each edge links
   * another row, each edge requires a new row reader.
   * 
   * @param target
   *          the object source to which we link edges
   * @param prop
   *          the edge property
   * @param edges
   *          the edges
   * @param rowReader
   *          the row reader
   * @param childTableReader
   *          the table reader for the child objects
   * @param level
   *          the assembly level
   * @throws IOException
   */
  protected void assembleExternalEdges(PlasmaDataObject target, long targetSequence,
      PlasmaProperty prop, EdgeReader collection, RowReader rowReader,
      TableReader childTableReader, int level) throws IOException {
    for (CellValues childValues : collection.getRowValues()) {

      // see if this row is locked during fetch, and wait for it
      Object rowLock = fetchLocks.get(childValues.getRowKey());
      if (rowLock != null) {
        synchronized (rowLock) {
          try {
            rowLock.wait();
          } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
          }
        }
      }

      RowReader existingChildRowReader = childTableReader.getRowReader(childValues.getRowKey());
      if (existingChildRowReader != null) {
        // If assembled this row root before,
        // just link it. The data is already complete.
        PlasmaDataObject existingChild = (PlasmaDataObject) existingChildRowReader
            .getRootDataObject();
        synchronized (existingChild) {
          synchronized (target) {
            link(existingChild, target, prop);
          }
        }
        continue;
      }

      // While fetching this node, another thread can fail to find an
      // existing row reader registered
      // above and fall through to this fetch, and therefore fetch the
      // same row, in addition
      // to attempting to create the same row reader below, causing an
      // error or warning
      // The second thread may be arriving at this node from another
      // property/edge and
      // therefore need to link from another edge above.
      fetchLocks.put(childValues.getRowKey(), new Object());

      this.assembleExternalEdge(childValues, collection, childTableReader, target, targetSequence,
          prop, level);

      rowLock = fetchLocks.remove(childValues.getRowKey());
      if (rowLock != null) {
        synchronized (rowLock) {
          rowLock.notifyAll();
        }
      } else {
        log.error("expected locked row key '" + childValues.getRowKey() + "' for edgeReader, "
            + collection);
      }
    }
  }

  protected void assembleExternalEdge(CellValues childValues, EdgeReader edgeReader,
      TableReader childTableReader, PlasmaDataObject source, long sourceSequence,
      PlasmaProperty sourceProperty, int level) throws IOException {

    if (log.isDebugEnabled())
      log.debug("traverse: (" + sourceProperty + ") ");

    PlasmaType subType = edgeReader.getSubType();
    if (subType == null)
      subType = edgeReader.getBaseType();

    PlasmaDataObject child = null;
    RowReader childRowReader = null;

    // need to reconstruct the original graph, so need original UUID
    GraphColumnKeyFactory keyFactory = this.getKeyFactory(subType);
    if (childValues.isCompleteSelection()) {
      UUID uuid = this.fetchRootUUID(childTableReader, keyFactory, subType, childValues);
      PlasmaType childType = this.fetchRootType(childTableReader, keyFactory, subType, childValues);
      synchronized (source) {
        child = createChild(source, sourceProperty, uuid, childType);
      }
      synchronized (childTableReader) {
        childRowReader = childTableReader.createRowReader(child, childValues);
      }
    } else {
      CellValues childResult = null;
      synchronized (childTableReader) {
        childResult = fetchGraph(childValues.getRowKeyAsBytes(), childTableReader, subType);
      }
      if (childResult.containsColumn(rootTableReader.getTableConfig()
          .getDataColumnFamilyNameBytes(), GraphMetaKey.TOMBSTONE.codeAsBytes())) {
        log.warn("ignoring toubstone result row '" + childValues.getRowKey() + "'");
        return; // ignore toumbstone edge
      }
      UUID uuid = this.fetchRootUUID(childTableReader, keyFactory, subType, childResult);
      PlasmaType childType = this.fetchRootType(childTableReader, keyFactory, subType, childResult);
      synchronized (source) { // sync cause can modify source
        child = createChild(source, sourceProperty, uuid, childType);
      }
      synchronized (childTableReader) {
        childRowReader = childTableReader.getRowReader(childResult.getRowKey());
        if (childRowReader == null)
          childRowReader = childTableReader.createRowReader(child, childResult);
      }
    }

    if (log.isDebugEnabled())
      log.debug("initialized external child: " + child);

    synchronized (this.distributedReader) {
      this.distributedReader.mapRowReader(childValues.getRowKey(), childRowReader);
    }

    traversals.add(new Traversal(child, sourceSequence, edgeReader, source, sourceProperty,
        childRowReader, true, level + 1));

  }

}
