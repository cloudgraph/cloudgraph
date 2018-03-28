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
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.io.CellValues;
import org.cloudgraph.hbase.io.DistributedReader;
import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.io.RowReader;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.recognizer.GraphRecognizerContext;
import org.cloudgraph.recognizer.GraphRecognizerSyntaxTreeAssembler;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.query.collector.Selection;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreConstants;

import commonj.sdo.Property;

/**
 * Assembles a data graph where one or more collections may be "sliced" based on
 * path predicates within the <a target="#"
 * href="http://plasma-sdo.org/org/plasma/query/collector/Selection.html"
 * >"selection graph"</a>.
 * <p>
 * Edges within a collection are "recognized" as members of a slice based on a
 * binary expression syntax tree assembled from the path predicate describing
 * the slice. While a path predicate may be quite complex resulting in any
 * number of logical, relational or wildcard binary expressions, a single slice
 * syntax tree is used to evaluate any number of edges within a collection.
 * Where edge opposite graph nodes are found within the current row, an edge
 * recognizer is used, but where edge opposite graph nodes are found "outside"
 * the current row, a graph recognizer is used.
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
 * @see LocalEdgeRecognizerSyntaxTreeAssembler
 * @see LocalEdgeRecognizerContext
 * @see GraphRecognizerSyntaxTreeAssembler
 * @see GraphRecognizerContext
 * @see GraphSliceSupport
 * @see org.cloudgraph.hbase.key.StatefullColumnKeyFactory
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class GraphSliceAssembler extends DistributedAssembler {

  private static Log log = LogFactory.getLog(GraphSliceAssembler.class);
  private int scanCount;
  private GraphSliceSupport slice;
  private Charset charset;

  public GraphSliceAssembler(PlasmaType rootType, Selection selection,
      DistributedReader distributedReader, Timestamp snapshotDate) {
    super(rootType, selection, distributedReader, snapshotDate);
    this.charset = Charset.forName(CoreConstants.UTF8_ENCODING);
    this.slice = new GraphSliceSupport(this.selection, this.snapshotDate,
        distributedReader.getConnection());
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

    assembleData(target, targetSequence, props, rowReader);

    TableReader tableReader = rowReader.getTableReader();
    TableMapping tableConfig = tableReader.getTableConfig();

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

      PlasmaType childType = (PlasmaType) prop.getType();

      // NOTE: can we have predicates on singular props?
      Where where = this.selection.getPredicate(prop);

      // boolean external = isExternal(edges, rowReader);
      if (!edgeReader.isExternal()) {
        // List<Long> sequences = edgeReader.getSequences();
        Set<Long> sequences = null;
        if (prop.isMany() && where != null) {
          sequences = this.slice.fetchSequences((PlasmaType) prop.getType(), where, rowReader);
          if (sequences.size() > 0) {
            // preload properties for the NEXT level into the current
            // row so we have something to assemble
            Set<Property> childProperies = this.selection.getInheritedProperties(prop.getType(),
                level + 1);
            this.slice.loadBySequenceList(sequences, childProperies, childType, rowReader,
                edgeReader);
          }
        } else {
          // preload properties for the NEXT level into the current
          // row so we have something to assemble
          sequences = new HashSet<Long>();
          for (Long seq : edgeReader.getSequences())
            sequences.add(seq);
          if (sequences.size() > 0) {
            Set<Property> childProperies = this.selection.getInheritedProperties(prop.getType(),
                level + 1);
            this.slice.loadBySequenceList(sequences, childProperies, childType, rowReader,
                edgeReader);
          }
        }
        if (sequences.size() > 0) {
          assembleEdges(target, targetSequence, prop, edgeReader, sequences, rowReader,
              rowReader.getTableReader(), rowReader, level);
        }
      } else {
        String childTable = edgeReader.getTable();
        TableReader externalTableReader = distributedReader.getTableReader(childTable);

        if (log.isDebugEnabled())
          if (!tableConfig.getName().equals(externalTableReader.getTableConfig().getName()))
            log.debug("switching row context from table: '" + tableConfig.getName()
                + "' to table: '" + externalTableReader.getTableConfig().getName() + "'");
        List<CellValues> resultRows = null;
        if (where != null) {
          resultRows = this.slice.filter(childType, level, edgeReader, where, rowReader,
              externalTableReader);
        } else {
          resultRows = edgeReader.getRowValues();
        }
        assembleExternalEdges(target, targetSequence, prop, edgeReader, rowReader, resultRows,
            externalTableReader, level);
      }
    }
  }

  private void assembleEdges(PlasmaDataObject target, long targetSequence, PlasmaProperty prop,
      EdgeReader edgeReader, Set<Long> sequences, RowReader rowReader,
      TableReader childTableReader, RowReader childRowReader, int level) throws IOException {
    for (long sequence : sequences) {

      PlasmaType subType = edgeReader.getSubType();
      if (subType == null)
        subType = edgeReader.getBaseType();

      if (childRowReader.contains(sequence, subType)) {
        // we've seen this child before so his data is complete, just
        // link
        PlasmaDataObject existingChild = (PlasmaDataObject) childRowReader.getDataObject(sequence,
            subType);
        link(existingChild, target, prop);
        continue;
      }

      if (log.isDebugEnabled())
        log.debug("local edge: " + target.getType().getURI() + "#" + target.getType().getName()
            + "->" + prop.getName() + " (" + sequence + ")");

      this.assembleEdge(sequence, edgeReader, childRowReader, target, targetSequence, prop, level);
    }
  }

  // Target is a different row, within this table or another.
  // Since we are assembling a graph, each edge requires
  // a new row reader.
  // each edge is a new root in the target table
  // so need a new row reader for each
  private void assembleExternalEdges(PlasmaDataObject target, long targetSequence,
      PlasmaProperty prop, EdgeReader collection, RowReader rowReader, List<CellValues> resultRows,
      TableReader childTableReader, int level) throws IOException {
    for (CellValues childValues : resultRows) {
      // for (String childRowKey : collection.getRowKeys()) {
      RowReader existingChildRowReader = childTableReader.getRowReader(childValues.getRowKey());
      if (existingChildRowReader != null) {
        // we've seen this child before so his data is complete, just
        // link
        PlasmaDataObject existingChild = (PlasmaDataObject) existingChildRowReader
            .getRootDataObject();
        link(existingChild, target, prop);
        continue;
      }
      // String childRowKeyStr = new String(childRowKey, this.charset);
      // if (resultRows != null && !resultRows.contains(childRowKey))
      // continue; // not found in predicate

      this.assembleExternalEdge(childValues, collection, childTableReader, target, targetSequence,
          prop, level);
    }
  }

}
