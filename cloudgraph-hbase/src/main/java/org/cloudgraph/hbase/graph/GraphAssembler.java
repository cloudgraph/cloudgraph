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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.hbase.io.DistributedReader;
import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.io.OperationException;
import org.cloudgraph.hbase.io.RowReader;
import org.cloudgraph.hbase.io.TableReader;
import org.plasma.query.collector.Selection;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

/**
 * Constructs a data graph starting with a given root SDO type based on a map of
 * selected SDO properties, where properties are mapped by selected types
 * required in the result graph.
 * <p>
 * The assembly is triggered by calling the
 * {@link GraphAssembler#assemble(Result resultRow)} method which recursively
 * reads HBase keys and values re-constituting the data graph. The assembly
 * traversal is driven by HBase column values representing the original edges or
 * containment structure of the graph.
 * </p>
 * <p>
 * Since every column key in HBase must be unique, and a data graph may contain
 * any number of nodes, a column key factory is used both to persist as well as
 * re-constitute a graph. A minimal amount of "state" information is therefore
 * stored with each graph which maps user readable sequence numbers (which are
 * used in column keys) to UUID values. The nodes of the resulting data graph
 * are re-created with the original UUID values.
 * </p>
 * 
 * @see org.cloudgraph.hbase.key.StatefullColumnKeyFactory
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class GraphAssembler extends DistributedAssembler {
  private static Log log = LogFactory.getLog(GraphAssembler.class);

  /**
   * Constructor.
   * 
   * @param rootType
   *          the SDO root type for the result data graph
   * @param selection
   *          selected SDO properties. Properties are mapped by selected types
   *          required in the result graph.
   * @param snapshotDate
   *          the query snapshot date which is populated into every data object
   *          in the result data graph.
   */
  public GraphAssembler(PlasmaType rootType, Selection selection,
      DistributedReader distributedReader, Timestamp snapshotDate) {
    super(rootType, selection, distributedReader, snapshotDate);
  }

  @Override
  protected void assemble(PlasmaDataObject target, long targetSequence,
      EdgeReader sourceCollection, PlasmaDataObject source, PlasmaProperty sourceProperty,
      RowReader rowReader, int level) throws IOException {
    Set<Property> props = this.getProperties(target, source, sourceProperty, level);
    if (props.size() == 0)
      return;
    if (log.isDebugEnabled())
      log.debug("assembling(" + level + "): " + target + ": " + props.toString());

    assembleData(target, targetSequence, props, rowReader);

    TableReader tableReader = rowReader.getTableReader();
    TableConfig tableConfig = tableReader.getTableConfig();

    // reference props
    for (Property p : props) {
      PlasmaProperty prop = (PlasmaProperty) p;
      if (prop.getType().isDataType())
        continue;

      EdgeReader edgeReader = null;
      if (rowReader.edgeExists((PlasmaType) target.getType(), prop, targetSequence)) {
        edgeReader = rowReader.getEdgeReader((PlasmaType) target.getType(), prop, targetSequence);
      }

      if (edgeReader == null)
        continue; // edge not found in data
      // }
      // else { // root
      // edgeReader =
      // rowReader.getEdgeReader((PlasmaType)target.getType(),
      // prop);
      // }

      // if (keyValue == null || keyValue.length == 0 ) {
      // continue; // zero length can happen on modification or delete as
      // we keep cell history
      // }
      // if (log.isDebugEnabled())
      // log.debug(prop.toString() + ": " + Bytes.toString(keyValue));

      // Edge[] edges = rowReader.getGraphState().unmarshalEdges(
      // keyValue);
      // if (edges.length == 0) {
      // continue; // zero length can happen on modification or delete as
      // we keep cell history
      // }

      if (!edgeReader.isExternal()) {
        assembleEdges(target, targetSequence, prop, edgeReader, rowReader, tableReader, rowReader,
            level);
      } else {
        // String childTable =
        // rowReader.getGraphState().getRowKeyTable(edges[0].getUuid());
        // if (childTable == null)
        // throw new OperationException("no table found for type, " +
        // edges[0].getType());
        TableReader externalTableReader = distributedReader.getTableReader(edgeReader.getTable());
        if (externalTableReader == null)
          throw new OperationException("no table reader found for type, "
              + edgeReader.getBaseType());
        assembleExternalEdges(target, targetSequence, prop, edgeReader, rowReader,
            externalTableReader, level);
      }
    }
  }

  protected void assembleEdges(PlasmaDataObject source, long sourceSequence,
      PlasmaProperty sourceProp, EdgeReader collection, RowReader rowReader,
      TableReader childTableReader, RowReader childRowReader, int level) throws IOException {
    // for (Edge edge : edges) {
    for (long sequence : collection.getSequences()) {

      if (log.isDebugEnabled())
        log.debug("reading local edge: " + source.getType() + "->" + sourceProp.getName() + " ("
            + sequence + ")");

      PlasmaType subType = collection.getSubType();
      if (subType == null)
        subType = collection.getBaseType();
      // UUID uuid = UUID.fromString(edge.getUuid());
      if (childRowReader.contains(sequence, subType)) {
        // we've seen this child before so his data is complete, just
        // link
        PlasmaDataObject existingChild = (PlasmaDataObject) childRowReader.getDataObject(sequence,
            collection.getBaseType());
        if (log.isDebugEnabled())
          log.debug("linking existing local child (sequence: " + sequence + "): " + existingChild);
        link(existingChild, source, sourceProp);
        continue;
      }

      // create a child object
      // PlasmaDataObject child = createChild(target, prop, edge);
      // childRowReader.addDataObject(child);

      this.assembleEdge(sequence, collection, childRowReader, source, sourceSequence, sourceProp,
          level);
    }
  }

  /**
   * Assembles a given set of edges where the target is a different row, within
   * this table or another. Since we are assembling a graph, each edge requires
   * a new row reader. Each edge is a new root in the target table so need a new
   * row reader for each.
   * 
   * @param source
   *          the object source to which we link edges
   * @param sourceProp
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
  protected void assembleExternalEdges(PlasmaDataObject source, long sourceSequence,
      PlasmaProperty sourceProp, EdgeReader collection, RowReader rowReader,
      TableReader childTableReader, int level) throws IOException {
    for (String childRowKey : collection.getRowKeys()) {
      if (log.isDebugEnabled())
        log.debug("reading external edge: " + source.getType() + "->" + sourceProp.getName()
            + "key ('" + childRowKey + "')");
      // need to look up an existing row reader based on the root UUID of
      // the external graph
      // or the row key, and the row key is all we have in the local graph
      // state. The edge UUID
      // is a local graph UUID.
      // byte[] childRowKey =
      // rowReader.getGraphState().getRowKey(edge.getUuid()); // use local
      // edge UUID
      RowReader existingChildRowReader = childTableReader.getRowReader(childRowKey);
      if (existingChildRowReader != null) {
        // If assembled this row root before,
        // just link it. The data is already complete.
        PlasmaDataObject existingChild = (PlasmaDataObject) existingChildRowReader
            .getRootDataObject();
        if (log.isDebugEnabled())
          log.debug("linking existing external child (key: " + childRowKey + "): " + existingChild);
        link(existingChild, source, sourceProp);
        continue;
      }

      this.assembleExternalEdge(childRowKey, collection, childTableReader, source, sourceSequence,
          sourceProp, level);
    }
  }

}
