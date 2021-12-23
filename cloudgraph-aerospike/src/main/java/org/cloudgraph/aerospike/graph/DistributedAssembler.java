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
package org.cloudgraph.aerospike.graph;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.aerospike.io.CellValues;
import org.cloudgraph.aerospike.io.DistributedReader;
import org.cloudgraph.aerospike.io.EdgeReader;
import org.cloudgraph.aerospike.io.RowReader;
import org.cloudgraph.aerospike.io.TableReader;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.common.concurrent.GraphMetricVisitor;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.collector.Selection;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreDataObject;

/**
 * Supports the assembly of a directed data graph which may span multiple HBase
 * tables and/or rows by maintaining a stack of row readers annotated with graph
 * "level" and other information. This allows a specific row reader to be
 * determined for any data object within a graph based entirely or in part on
 * its level within the graph. This is essential for data object types which are
 * not configured as root types within any table, and yet a specific row reader
 * must be determined.
 * 
 * @see org.cloudgraph.aerospike.io.DistributedReader
 * @see org.cloudgraph.aerospike.io.TableReader
 * @see org.cloudgraph.aerospike.io.RowReader
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public abstract class DistributedAssembler extends DefaultAssembler implements HBaseGraphAssembler {
  private static Log log = LogFactory.getLog(DistributedAssembler.class);

  protected DistributedReader distributedReader;

  /**
   * Constructor.
   * 
   * @param rootType
   *          the distributed graph root type
   * @param selection
   *          the selection collector
   * @param distributedReader
   *          the distributed reader
   * @param snapshotDate
   *          the query snapshot date
   */
  public DistributedAssembler(PlasmaType rootType, Selection selection,
      DistributedReader distributedReader, Timestamp snapshotDate) {
    super(rootType, selection, distributedReader.getRootTableReader(), snapshotDate);
    this.distributedReader = distributedReader;
  }

  /**
   * Recursively re-constitutes a data graph distributed across multiple HBase
   * tables and/or rows, starting with the given HBase client result row.
   * <p>
   * To retrieve the graph use {@link GraphAssembler#getDataGraph()}. a map of
   * selected SDO properties. Properties are mapped by selected types required
   * in the result graph.
   * </p>
   * 
   * @param resultRow
   *          the HBase client result (row).
   */
  @Override
  public void assemble(CellValues resultRow) {

    this.root = createRoot(this.getKeyFactory(this.rootType), resultRow);

    RowReader rowReader = this.rootTableReader.createRowReader(this.root, resultRow);

    this.distributedReader.mapRowReader(this.root, rowReader);
    // FIXME: are there not supposed to be instance
    // properties on data object? Why must we
    // go into core object.
    CoreDataObject root = (CoreDataObject) this.root;
    root.getValueObject().put(CloudGraphConstants.ROW_KEY, rowReader.getRowKey());
    root.getValueObject().put(CloudGraphConstants.SEQUENCE, CloudGraphConstants.ROOT_SEQUENCE);

    long before = System.currentTimeMillis();
    try {
      assemble(this.root, CloudGraphConstants.ROOT_SEQUENCE, null, null, null, rowReader, 0);
    } catch (IOException e) {
      throw new GraphServiceException(e);
    }
    long after = System.currentTimeMillis();

    root.getValueObject()
        .put(CloudGraphConstants.GRAPH_ASSEMBLY_TIME, Long.valueOf(after - before));

    GraphMetricVisitor visitor = new GraphMetricVisitor();
    this.root.accept(visitor);

    root.getValueObject().put(CloudGraphConstants.GRAPH_NODE_COUNT,
        Long.valueOf(visitor.getCount()));
    root.getValueObject().put(CloudGraphConstants.GRAPH_DEPTH, Long.valueOf(visitor.getDepth()));
    root.getValueObject().put(CloudGraphConstants.GRAPH_THREAD_COUNT,
        Long.valueOf(visitor.getThreadCount()));

    List<String> tables = new ArrayList<String>();
    for (TableReader tableReader : this.distributedReader.getTableReaders()) {
      tables.add(tableReader.getQualifiedPhysicalTableName()); // FIXME: should
                                                               // be relative
                                                               // name
    }
    root.getValueObject().put(CloudGraphConstants.GRAPH_TABLE_NAMES, tables);
  }

  /**
   * Populates the given data object target, recursively fetching data for and
   * linking related data objects which make up the resulting directed graph.
   * 
   * @param targetSequence
   *          the current data object sequence
   * @param source
   *          the source or parent data object
   * @param sourceProperty
   *          the source (reference) property
   * @param rowReader
   *          the current row reader
   * @param level
   *          the current graph level
   * @throws IOException
   *           if a remote or network exception occurs.
   */
  protected abstract void assemble(PlasmaDataObject target, long targetSequence,
      EdgeReader collection, PlasmaDataObject source, PlasmaProperty sourceProperty,
      RowReader rowReader, int level) throws IOException;

  protected void assembleEdge(long childSequence, EdgeReader collection, RowReader childRowReader,
      PlasmaDataObject source, long sourceSequence, PlasmaProperty sourceProperty, int level)
      throws IOException {

    PlasmaType subType = collection.getSubType();
    if (subType == null)
      subType = collection.getBaseType();
    this.distributedReader.mapRowReader(childSequence, subType, childRowReader);

    if (log.isDebugEnabled())
      log.debug("traverse: (" + sourceProperty + ") ");
    UUID uuid = this.fetchUUID(subType, childSequence, collection, childRowReader);
    PlasmaType childType = this.fetchType(subType, childSequence, collection, childRowReader);
    PlasmaDataObject child = createChild(uuid, childType, childSequence, source, sourceProperty);
    // PlasmaDataObject child = createChild(childSequence, collection,
    // source, sourceProperty, childRowReader, this.graph);
    // assembleUUID(child, childSequence, collection, childRowReader);
    if (log.isDebugEnabled())
      log.debug("initialized local child: " + child);
    if (!childRowReader.contains(child))
      childRowReader.addDataObject(child);
    if (!childRowReader.contains(child, childSequence, (PlasmaType) child.getType()))
      childRowReader.addDataObject(child, childSequence, (PlasmaType) child.getType());

    assemble(child, childSequence, collection, source, sourceProperty, childRowReader, level + 1);

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
      child = createChild(source, sourceProperty, uuid, childType);
      childRowReader = childTableReader.createRowReader(child, childValues);
    } else {
      CellValues childResult = fetchGraph(childValues.getRowKeyAsBytes(), childTableReader, subType);
      if (childResult.containsColumn(rootTableReader.getTableConfig()
          .getDataColumnFamilyNameBytes(), GraphMetaKey.TOMBSTONE.codeAsBytes())) {
        log.warn("ignoring toubstone result row '" + childValues.getRowKey() + "'");
        return; // ignore toumbstone edge
      }
      UUID uuid = this.fetchRootUUID(childTableReader, keyFactory, subType, childResult);
      PlasmaType childType = this.fetchRootType(childTableReader, keyFactory, subType, childResult);
      child = createChild(source, sourceProperty, uuid, childType);
      childRowReader = childTableReader.createRowReader(child, childResult);
    }

    if (log.isDebugEnabled())
      log.debug("initialized external child: " + child);

    this.distributedReader.mapRowReader(childValues.getRowKey(), childRowReader);

    // assemble after minimal object created
    assemble(child, CloudGraphConstants.ROOT_SEQUENCE, edgeReader, source, sourceProperty,
        childRowReader, level + 1);

  }

  /**
   * Resets the assembler.
   */
  @Override
  public void clear() {
    this.root = null;
    this.distributedReader.clear();
  }
}
