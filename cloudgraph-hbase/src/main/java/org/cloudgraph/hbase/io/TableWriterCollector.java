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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.mapping.MappingConfiguration;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.DeletedObjectCollector;
import org.plasma.sdo.access.provider.common.ModifiedObjectCollector;

import commonj.sdo.ChangeSummary;
import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;

/**
 * Traverses the change summary of the given <s
 * href="http://docs.plasma-sdo.org/api/org/plasma/sdo/PlasmaDataGraph.html"
 * >Data Graph<a> collecting a list of {@link TableWriter} elements with nested
 * {@link RowWriter} elements. Each {@link RowWriter} represents the root of a
 * new or existing <a
 * href="http://docs.plasma-sdo.org/api/org/plasma/sdo/PlasmaDataGraph.html"
 * >Data Graph</a> within the HBase table associated with the parent
 * {@link TableWriter}.
 * <p>
 * Graph nodes which are "unbound" (not assigned directly to a specific table)
 * are assigned based on the first parent node within the graph. Other parent
 * nodes (if exist) are ignored.
 * </p>
 * 
 * @see org.cloudgraph.store.mapping.StoreMapping
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.hbase.io.TableWriter
 * @see org.cloudgraph.hbase.io.RowWriter
 * @see commonj.sdo.ChangeSummary
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class TableWriterCollector extends WriterSupport {

  private static Log log = LogFactory.getLog(TableWriterCollector.class);
  private DataGraph dataGraph;
  private ChangeSummary changeSummary;
  private PlasmaDataObject root;
  private TableWriter rootTableWriter;
  private Map<String, TableWriter> result = new HashMap<String, TableWriter>();
  private PlasmaDataObject[] created;
  private ModifiedObjectCollector modified;
  private DeletedObjectCollector deleted;
  private MappingConfiguration mapping = StoreMapping.getInstance();

  public TableWriterCollector(DataGraph dataGraph, PlasmaDataObject[] created,
      ModifiedObjectCollector modified, DeletedObjectCollector deleted,
      StoreMappingContext mappingContext) throws IOException {
    super(mappingContext);
    this.dataGraph = dataGraph;
    this.changeSummary = dataGraph.getChangeSummary();
    this.root = (PlasmaDataObject) dataGraph.getRootObject();
    this.created = created;
    this.modified = modified;
    this.deleted = deleted;
    collect();
  }

  public List<TableWriter> getTableWriters() {
    List<TableWriter> list = new ArrayList<TableWriter>(this.result.size());
    for (TableWriter table : this.result.values())
      list.add(table);
    return list;
  }

  public TableWriter getRootTableWriter() {
    return rootTableWriter;
  }

  public Map<DataObject, RowWriter> getRowWriterMap() {
    return this.rowWriterMap;
  }

  private void collect() throws IOException {
    // May need opposite writers just for sake of getting its
    // row writer and getting the row key to link into
    // modified graph.
    // FIXME: how do we detect this condition? Can we create
    // a row reader here instead of a writer??

    // Collect all "bound" data objects up front
    // Note: The root may NOT be part of the change summary, but regardless
    // it must be associated with a table in order to
    // associate any linked un-bound types
    TableMapping rootTable = this.mapping.findTable(this.root.getType(), this.mappingContext);
    if (rootTable != null)
      associate(rootTable, this.root);

    // Collect all "bound" data objects associated w/the change summary
    for (DataObject dataObject : this.changeSummary.getChangedDataObjects()) {
      PlasmaType type = (PlasmaType) dataObject.getType();
      TableMapping table = this.mapping.findTable(type, this.mappingContext);
      if (table != null)
        associate(table, dataObject);
    }

    if (result.size() == 0)
      throw new OperationException("no configured table(s) could be associated with root type "
          + this.root.getType().toString() + " - please add a configuration for this type");

    for (PlasmaDataObject dataObject : this.created) {
      PlasmaType type = (PlasmaType) dataObject.getType();
      TableMapping table = this.mapping.findTable(type, this.mappingContext);
      if (table == null) {
        if (log.isDebugEnabled())
          log.debug("collecting unbound created: " + dataObject);
        associate(dataObject);
      } else {
        if (log.isDebugEnabled())
          log.debug("collecting bound created: " + dataObject);
      }
    }

    for (PlasmaDataObject dataObject : this.modified.getResult()) {
      PlasmaType type = (PlasmaType) dataObject.getType();
      TableMapping table = this.mapping.findTable(type, this.mappingContext);
      if (table == null) {
        if (log.isDebugEnabled())
          log.debug("collecting unbound modified: " + dataObject);
        associate(dataObject);
      } else {
        if (log.isDebugEnabled())
          log.debug("collecting bound modified: " + dataObject);
      }
    }

    List<PlasmaDataObject> deletedResult = this.deleted.getResult();
    for (int i = deletedResult.size() - 1; i >= 0; i--) {
      PlasmaDataObject dataObject = deletedResult.get(i);
      PlasmaType type = (PlasmaType) dataObject.getType();
      TableMapping table = this.mapping.findTable(type, this.mappingContext);
      if (table == null) {
        if (log.isDebugEnabled())
          log.debug("collecting unbound deleted: " + dataObject);
        associate(dataObject);
      } else {
        if (log.isDebugEnabled())
          log.debug("collecting bound deleted: " + dataObject);
      }
    }
  }

  /**
   * Links the given "unbound" data object to a {@link RowWriter row writer}.
   * The containment ancestry is searched and for the first container with a
   * "bound" type, if an existing associated {@link RowWriter row writer} exists
   * for the bound type, that row writer is returned. Otherwise the bound
   * container data object is associated. Bound data objects in the ancestry may
   * not be associated if they are not part of the changed object set within the
   * change summary.
   * <p>
   * </p>
   * An unbound data object is not directly associated with a table but only as
   * part of a containment hierarchy within a graph.
   * 
   * @param target
   *          the data object
   * @throws IOException
   */
  private void associate(DataObject target) throws IOException {
    TableMapping table = this.mapping.findTable((PlasmaType) target.getType(), this.mappingContext);
    if (table != null)
      throw new IllegalArgumentException("expected unbound data object - given data object "
          + target + " is bound to table, " + table.getQualifiedPhysicalName());

    RowWriter rowWriter = this.rowWriterMap.get(target);
    if (rowWriter == null) {
      RowWriter containerRowWriter = null;
      for (DataObject container : this.getContainerAncestry(target)) {
        TableMapping containerTable = this.mapping.findTable((PlasmaType) container.getType(),
            this.mappingContext);
        if (containerTable == null)
          continue; // need a bound DO to associate to
        containerRowWriter = this.rowWriterMap.get(container);
        // Its container is a bound type but the container DO is not
        // part of the
        // changed object set
        if (containerRowWriter == null) {
          containerRowWriter = associate(containerTable, container);
        }
        break;
      }
      if (containerRowWriter == null)
        throw new OperationException("no row writer could be associated with data object, "
            + target + ", or its containment ancestry");
      rowWriter = containerRowWriter;
      if (log.isDebugEnabled())
        log.debug("associating " + target + " with table '"
            + rowWriter.getTableWriter().getTableConfig().getQualifiedPhysicalName() + "'");
      rowWriter.addDataObject(target);
      this.rowWriterMap.put(target, rowWriter);
    } else {
      if (log.isDebugEnabled())
        log.debug("type " + target.getType() + " already associated with table '"
            + rowWriter.getTableWriter().getTableConfig().getQualifiedPhysicalName()
            + "' by means of another source/parent");
    }
  }

  /**
   * Links the given "bound" data object to a row writer. A bound data object is
   * directly associated with a table
   * 
   * @param target
   *          the data object
   * @return the row writer.
   */
  private RowWriter associate(TableMapping table, DataObject target) throws IOException {

    // a table is configured with this type as root
    TableWriter tableWriter = (TableWriter) result.get(table.getQualifiedLogicalName());
    RowWriter rowWriter = null;
    if (tableWriter == null) {
      tableWriter = new GraphTableWriter(table, this.mappingContext);
      rowWriter = createRowWriter(tableWriter, target);
      tableWriter = rowWriter.getTableWriter();
      if (log.isDebugEnabled())
        log.debug("adding " + tableWriter.getTableConfig().getQualifiedLogicalName());
      result.put(tableWriter.getTableConfig().getQualifiedLogicalName(), tableWriter);
    } else { // just add a row writer to existing table writer
      rowWriter = this.addRowWriter(target, tableWriter);
    }
    if (log.isDebugEnabled())
      log.debug("associating (root) " + target.getType().getURI() + "#"
          + target.getType().getName() + " with table '"
          + rowWriter.getTableWriter().getTableConfig().getQualifiedLogicalName() + "'");

    this.rowWriterMap.put(target, rowWriter);

    if (this.dataGraph.getRootObject().equals(target)) // root object
      this.rootTableWriter = tableWriter;

    return rowWriter;
  }

}
