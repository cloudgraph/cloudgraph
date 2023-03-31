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
package org.cloudgraph.core.mutation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.Bytes;
import org.cloudgraph.core.Connection;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.io.DistributedGraphWriter;
import org.cloudgraph.core.io.DistributedWriter;
import org.cloudgraph.core.io.RowWriter;
import org.cloudgraph.core.io.TableWriter;
import org.cloudgraph.core.io.TableWriterCollector;
//import org.cloudgraph.hbase.io.DistributedGraphWriter;
//import org.cloudgraph.hbase.io.TableWriterCollector;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.service.CreatedCommitComparator;
import org.plasma.sdo.AssociationPath;
import org.plasma.sdo.PlasmaChangeSummary;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.DeletedObjectCollector;
import org.plasma.sdo.access.provider.common.ModifiedObjectCollector;
import org.plasma.sdo.core.CoreDataObject;
import org.plasma.sdo.core.SnapshotMap;
import org.plasma.sdo.repository.Class_;
import org.plasma.sdo.repository.Classifier;
import org.plasma.sdo.repository.PlasmaRepository;

import sorts.InsertionSort;
import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;
import commonj.sdo.Property;

/**
 * Traverses the change summary for one or more data graphs and collects changes
 * in the form of HBase row mutations.
 * <p>
 * For each graph: - performs any change summary ordering - collects table
 * writers based on graph metadata and configuration information - assembles
 * table writers for the graph into a single graph writer composed of table
 * writers, which are composed of row writers - passes each changed object
 * (created, modified, deleted) along with the graph writer to logic within this
 * class. - marshals out the state for each changed row after all changes
 * complete in to the state column - for each row, detects if the root object is
 * deleted, and then adds a toumbsone column
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public class GraphMutationCollector extends DefaultMutation implements MutationCollector {
  private static Log log = LogFactory.getLog(GraphMutationCollector.class);
  protected Create create;
  protected Update update;
  protected Delete delete;

  public GraphMutationCollector(ServiceContext context, SnapshotMap snapshotMap, String username) {
    super(context, snapshotMap, username);
    this.create = new Create(context, snapshotMap, username);
    this.update = new Update(context, snapshotMap, username);
    this.delete = new Delete(context, snapshotMap, username);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.mutation.MutationCollector#close()
   */
  @Override
  public void close() {
    this.context.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.mutation.MutationCollector#collectChanges(commonj
   * .sdo.DataGraph)
   */
  @Override
  public Map<TableWriter, Map<String, Mutations>> collectChanges(DataGraph dataGraph,
      Connection connection) throws IOException, IllegalAccessException {
    Map<TableWriter, Map<String, Mutations>> mutations = new HashMap<TableWriter, Map<String, Mutations>>();

    PlasmaChangeSummary changeSummary = (PlasmaChangeSummary) dataGraph.getChangeSummary();
    if (log.isDebugEnabled())
      log.debug(changeSummary.toString());

    if (changeSummary.getChangedDataObjects().size() == 0) {
      log.warn("no changes detected");
      return mutations;
    }

    PlasmaDataObject[] created = sortCreated(changeSummary);

    ModifiedObjectCollector modified = new ModifiedObjectCollector(dataGraph);
    DeletedObjectCollector deleted = new DeletedObjectCollector(dataGraph);

    TableWriterCollector collector = new TableWriterCollector(dataGraph, created, modified,
        deleted, this.context);

    DistributedGraphWriter graphWriter = new DistributedGraphWriter(dataGraph, collector,
        connection, this.context);

    this.create(dataGraph, created, graphWriter);
    this.modify(dataGraph, modified, graphWriter);
    this.delete(dataGraph, deleted, graphWriter);

    for (TableWriter tableWriter : graphWriter.getTableWriters()) {
      Map<String, Mutations> rowMutations = mutations.get(tableWriter);
      if (rowMutations == null) {
        rowMutations = new HashMap<String, Mutations>();
        mutations.put(tableWriter, rowMutations);
      }

      for (RowWriter rowWriter : tableWriter.getAllRowWriters()) {
        addRemoveStateColumns(tableWriter, rowWriter);
        collectRowMutations(tableWriter, rowWriter, rowMutations);
      }
    }
    return mutations;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.mutation.MutationCollector#collectChanges(commonj
   * .sdo.DataGraph[])
   */
  @Override
  public Map<TableWriter, Map<String, Mutations>> collectChanges(DataGraph[] dataGraphs,
      Connection connection) throws IOException, IllegalAccessException {
    Map<TableWriter, Map<String, Mutations>> mutations = new HashMap<TableWriter, Map<String, Mutations>>();

    boolean hasChanges = false;
    for (DataGraph dataGraph : dataGraphs) {
      PlasmaChangeSummary changeSummary = (PlasmaChangeSummary) dataGraph.getChangeSummary();
      if (log.isDebugEnabled())
        log.debug(changeSummary.toString());
      if (changeSummary.getChangedDataObjects().size() > 0) {
        hasChanges = true;
        break;
      }
    }

    if (!hasChanges) {
      log.warn("no changes detected");
      return mutations;
    }

    List<DistributedWriter> graphWriters = new ArrayList<DistributedWriter>();
    for (DataGraph dataGraph : dataGraphs) {
      PlasmaChangeSummary changeSummary = (PlasmaChangeSummary) dataGraph.getChangeSummary();

      PlasmaDataObject[] created = sortCreated(changeSummary);

      ModifiedObjectCollector modified = new ModifiedObjectCollector(dataGraph);
      DeletedObjectCollector deleted = new DeletedObjectCollector(dataGraph);

      TableWriterCollector collector = new TableWriterCollector(dataGraph, created, modified,
          deleted, this.context);

      DistributedWriter graphWriter = new DistributedGraphWriter(dataGraph, collector, connection,
          this.context);
      graphWriters.add(graphWriter);

      this.create(dataGraph, created, graphWriter);
      this.modify(dataGraph, modified, graphWriter);
      this.delete(dataGraph, deleted, graphWriter);

    }

    for (DistributedWriter graphWriter : graphWriters) {

      for (TableWriter tableWriter : graphWriter.getTableWriters()) {
        Map<String, Mutations> rowMutations = mutations.get(tableWriter);
        if (rowMutations == null) {
          rowMutations = new HashMap<String, Mutations>();
          mutations.put(tableWriter, rowMutations);
        }

        for (RowWriter rowWriter : tableWriter.getAllRowWriters()) {
          if (log.isDebugEnabled())
            log.debug("commiting data object: " + rowWriter.getRootDataObject().toString());
          addRemoveStateColumns(tableWriter, rowWriter);
          collectRowMutations(tableWriter, rowWriter, rowMutations);
        }
      }
    }

    return mutations;
  }

  private void modify(DataGraph dataGraph, ModifiedObjectCollector modified,
      DistributedWriter graphWriter) throws IllegalAccessException, IOException {
    for (PlasmaDataObject dataObject : modified.getResult()) {
      RowWriter rowWriter = graphWriter.getRowWriter(dataObject);
      TableWriter tableWriter = rowWriter.getTableWriter();
      this.update.init(dataGraph, dataObject, rowWriter);
      this.update.validate(dataGraph, dataObject, rowWriter);
      this.update.setup(dataGraph, dataObject, rowWriter);
      this.update.collect(dataGraph, dataObject, graphWriter, tableWriter, rowWriter);
    }
  }

  private void delete(DataGraph dataGraph, DeletedObjectCollector deleted,
      DistributedWriter graphWriter) throws IllegalAccessException, IOException {
    for (PlasmaDataObject dataObject : deleted.getResult()) {

      RowWriter rowWriter = graphWriter.getRowWriter(dataObject);
      TableWriter tableWriter = rowWriter.getTableWriter();
      this.delete.init(dataGraph, dataObject, rowWriter);
      this.delete.validate(dataGraph, dataObject, rowWriter);
      this.delete.setup(dataGraph, dataObject, rowWriter);
      this.delete.collect(dataGraph, dataObject, graphWriter, tableWriter, rowWriter);
      // rowWriter.getGraphState().removeSequence(dataObject);
    }
  }

  private void create(DataGraph dataGraph, PlasmaDataObject[] created, DistributedWriter graphWriter)
      throws IOException, IllegalAccessException {

    for (PlasmaDataObject dataObject : created) {
      RowWriter rowWriter = graphWriter.getRowWriter(dataObject);
      TableWriter tableWriter = rowWriter.getTableWriter();
      this.create.init(dataGraph, dataObject, rowWriter);
      this.create.validate(dataGraph, dataObject, rowWriter);
      this.create.setup(dataGraph, dataObject, rowWriter);
      this.create.collect(dataGraph, dataObject, graphWriter, tableWriter, rowWriter);
    }
  }

  private PlasmaDataObject[] sortCreated(PlasmaChangeSummary changeSummary) {
    List<CoreDataObject> createdList = new ArrayList<CoreDataObject>();
    for (DataObject changed : changeSummary.getChangedDataObjects()) {
      if (changeSummary.isCreated(changed))
        createdList.add((CoreDataObject) changed);
    }
    CoreDataObject[] createdArray = new CoreDataObject[createdList.size()];
    createdList.toArray(createdArray);
    Comparator<CoreDataObject> comparator = new CreatedCommitComparator();
    InsertionSort sort = new InsertionSort();
    sort.sort(createdArray, comparator);
    PlasmaDataObject[] created = new PlasmaDataObject[createdArray.length];
    for (int i = 0; i < createdArray.length; i++)
      created[i] = createdArray[i];
    return created;
  }

  private void collectRowMutations(TableWriter tableWriter, RowWriter rowWriter,
      Map<String, Mutations> rowMutations) throws IOException {
    rowMutations.put(Bytes.toString(rowWriter.getRowKey()), rowWriter.getWriteOperations());
  }

  private void addRemoveStateColumns(TableWriter tableWriter, RowWriter rowWriter)
      throws IOException {
    byte[] fam = tableWriter.getTableConfig().getDataColumnFamilyNameBytes();
    String rootUUID = ((PlasmaDataObject) rowWriter.getRootDataObject()).getUUIDAsString();
    if (rowWriter.isRootCreated()) {
      // rowWriter.getRow().addColumn(tableWriter.getTableConfig().getDataColumnFamilyNameBytes(),
      // GraphMetaField.__RU__.asBytes(),
      // Bytes.toBytes(rootUUID));
      // rowWriter.getRow().addColumn(tableWriter.getTableConfig().getDataColumnFamilyNameBytes(),
      // GraphMetaField.__RT__.asBytes(),
      // rowWriter.encodeRootType());
      rowWriter.writeRowData(fam, GraphMetaKey.TIMESTAMP.codeAsBytes(),
          GraphMetaKey.TIMESTAMP.getStorageType(),
          Bytes.toBytes(String.valueOf(this.snapshotMap.getSnapshotNannoTime())));

      if (tableWriter.getTableConfig().tombstoneRowsOverwriteable()) {
        rowWriter.deleteRowData( // deletes all versions
            fam, GraphMetaKey.TOMBSTONE.codeAsBytes());
      }
    }

    if (!rowWriter.isRootDeleted()) {
      // String xml = rowWriter.getSequenceMapping().marshalAsString();
      byte[] bytes = rowWriter.getSequenceMapping().marshal();
      if (rowWriter.getSequenceMapping().isUpdated()) {
        rowWriter.writeRowData(fam, GraphMetaKey.SEQUENCE_MAPPING.codeAsBytes(),
            GraphMetaKey.SEQUENCE_MAPPING.getStorageType(), bytes);
      }
    } else { // root is deleted
      if (log.isDebugEnabled())
        log.debug("processing deleted root (" + rootUUID + ")");
      if (tableWriter.getTableConfig().tombstoneRows()) {
        // since this is the root, and someone could be pointing at us,
        // add a tombstone column
        if (log.isDebugEnabled())
          log.debug("adding toumbstone for root " + rowWriter.getRootDataObject().toString());
        rowWriter.writeRowData(fam, GraphMetaKey.TOMBSTONE.codeAsBytes(),
            GraphMetaKey.TOMBSTONE.getStorageType(),
            Bytes.toBytes(this.snapshotMap.getSnapshotDate().getTime()));
        rowWriter.deleteRowData( // deletes all version
            fam, GraphMetaKey.SEQUENCE_MAPPING.codeAsBytes());
      }
    }
  }

  private boolean parentLinksDeleted(PlasmaChangeSummary changeSummary, DataObject root) {
    boolean allDeleted = true;
    PlasmaType rootType = (PlasmaType) root.getType();
    List<String> uris = PlasmaRepository.getInstance().getAllNamespaceUris();
    for (String uri : uris) {
      List<Classifier> classifiers = PlasmaRepository.getInstance().getClassifiers(uri);
      for (Classifier c : classifiers) {
        if (c instanceof Class_) {
          ((Class_) rootType.getClassifier()).isRelation(((Class_) c),
              AssociationPath.singular_binary);
        }
      }
    }

    return allDeleted;
  }

  /**
   * Determines if all data object linked to every property where the root type
   * is its source have been deleted
   * 
   * @param changeSummary
   *          the change summary
   * @param root
   *          the root data object
   * @return whether all data object linked to every property where the root
   *         type is its source have been deleted
   */
  private boolean childLinksDeleted(PlasmaChangeSummary changeSummary, DataObject root) {
    boolean allDeleted = true;
    PlasmaType rootType = (PlasmaType) root.getType();
    for (Property rootProp : rootType.getProperties()) {
      if (rootProp.getType().isDataType())
        continue;
      if (rootProp.isMany()) {
        @SuppressWarnings("unchecked")
        List<DataObject> list = root.getList(rootProp);
        for (DataObject other : list)
          if (!changeSummary.isDeleted(other))
            allDeleted = false;
      } else {
        DataObject other = (DataObject) root.getDataObject(rootProp);
        if (other != null) {
          if (!changeSummary.isDeleted(other))
            allDeleted = false;
        } else {
          // root has a property, but how to tell if its used even
          log.warn("potentially incomplete delete graph detected for type, " + rootProp.getType()
              + " within root graph type " + rootType + " - ");
        }
      }
    }
    return allDeleted;
  }

}
