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
package org.cloudgraph.hbase.graph;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.common.concurrent.GraphMetricVisitor;
import org.cloudgraph.hbase.io.DistributedReader;
import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.io.RowReader;
import org.cloudgraph.hbase.io.TableReader;
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
 * @see org.cloudgraph.hbase.io.DistributedReader
 * @see org.cloudgraph.hbase.io.TableReader
 * @see org.cloudgraph.hbase.io.RowReader
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
  public void assemble(Result resultRow) {

    this.root = createRoot(this.rootKeyFactory, resultRow);

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
      tables.add(tableReader.getTableName());
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

    // childRowReader.addDataObject(child);
    // add it after minimally created with UUID

    assemble(child, childSequence, collection, source, sourceProperty, childRowReader, level + 1);

    // assemble(child, childSequence, target, prop, childRowReader,
    // level+1);
  }

  protected void assembleExternalEdge(String childRowKey, EdgeReader collection,
  /* RowReader childRowReader, */TableReader childTableReader, PlasmaDataObject source,
      long sourceSequence, PlasmaProperty sourceProperty, int level) throws IOException {

    if (log.isDebugEnabled())
      log.debug("traverse: (" + sourceProperty + ") ");

    PlasmaType subType = collection.getSubType();
    if (subType == null)
      subType = collection.getBaseType();

    Result childResult = fetchGraph(Bytes.toBytes(childRowKey), childTableReader, subType);
    if (childResult.containsColumn(rootTableReader.getTableConfig().getDataColumnFamilyNameBytes(),
        GraphMetaKey.TOMBSTONE.codeAsBytes())) {
      log.warn("ignoring toubstone result row '" + childRowKey + "'");
      return; // ignore toumbstone edge
    }

    // need to reconstruct the original graph, so need original UUID
    UUID uuid = this.fetchRootUUID(childTableReader, this.rootKeyFactory, subType, childResult);
    PlasmaType childType = this.fetchRootType(childTableReader, this.rootKeyFactory, subType,
        childResult);

    // create a child object using UUID from external row root
    PlasmaDataObject child = createChild(source, sourceProperty, uuid, childType);
    if (log.isDebugEnabled())
      log.debug("initialized external child: " + child);

    RowReader childRowReader = childTableReader.createRowReader(child, childResult);
    this.distributedReader.mapRowReader(childRowKey, childRowReader);

    // add after minimal assembly
    // childRowReader.addDataObject(child);

    assemble(child, CloudGraphConstants.ROOT_SEQUENCE, collection, source, sourceProperty,
        childRowReader, level + 1);

  }

  //
  // /**
  // * Peeks at the first edge and determines whether
  // * an external edge collection
  // * @param edges the state edges
  // * @param rowReader the reader
  // * @return whether
  // * an external edge collection
  // * @throws IOException
  // */
  // protected boolean isExternal(Edge[] edges, RowReader rowReader) throws
  // IOException {
  // if (edges.length > 0) {
  // return rowReader.getGraphState().findRowKey(
  // edges[0].getUuid()) != null;
  // }
  // else {
  // return false;
  // }
  // }

  /**
   * Resets the assembler.
   */
  @Override
  public void clear() {
    this.root = null;
    this.distributedReader.clear();
  }
}
