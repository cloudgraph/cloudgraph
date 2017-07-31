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
package org.cloudgraph.hbase.mutation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.io.DistributedGraphWriter;
import org.cloudgraph.hbase.io.DistributedWriter;
import org.cloudgraph.hbase.io.RowWriter;
import org.cloudgraph.hbase.io.TableWriter;
import org.cloudgraph.hbase.io.TableWriterCollector;
import org.cloudgraph.hbase.service.ServiceContext;
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
public class GraphMutationCollector extends DefaultMutation
		implements
			MutationCollector {
	private static Log log = LogFactory.getLog(GraphMutationCollector.class);
	protected Create create;
	protected Update update;
	protected Delete delete;
	public GraphMutationCollector(ServiceContext context,
			SnapshotMap snapshotMap, String username) {
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
	 * @see
	 * org.cloudgraph.hbase.mutation.MutationCollector#collectChanges(commonj
	 * .sdo.DataGraph)
	 */
	@Override
	public Map<TableWriter, List<Row>> collectChanges(DataGraph dataGraph)
			throws IOException, IllegalAccessException {
		Map<TableWriter, List<Row>> mutations = new HashMap<TableWriter, List<Row>>();

		PlasmaChangeSummary changeSummary = (PlasmaChangeSummary) dataGraph
				.getChangeSummary();
		if (log.isDebugEnabled())
			log.debug(changeSummary.toString());

		if (changeSummary.getChangedDataObjects().size() == 0) {
			log.warn("no changes detected");
			return mutations;
		}

		for (DataObject changed : changeSummary.getChangedDataObjects())
			this.checkConcurrency(dataGraph, (PlasmaDataObject) changed);

		PlasmaDataObject[] created = sortCreated(changeSummary);

		ModifiedObjectCollector modified = new ModifiedObjectCollector(
				dataGraph);
		DeletedObjectCollector deleted = new DeletedObjectCollector(dataGraph);

		TableWriterCollector collector = new TableWriterCollector(dataGraph,
				created, modified, deleted);

		DistributedWriter graphWriter = new DistributedGraphWriter(dataGraph,
				collector, this.context.getMarshallingContext());

		this.create(dataGraph, created, graphWriter);
		this.modify(dataGraph, modified, graphWriter);
		this.delete(dataGraph, deleted, graphWriter);

		for (TableWriter tableWriter : graphWriter.getTableWriters()) {
			List<Row> rowMutations = mutations.get(tableWriter);
			if (rowMutations == null) {
				rowMutations = new ArrayList<Row>();
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
	 * @see
	 * org.cloudgraph.hbase.mutation.MutationCollector#collectChanges(commonj
	 * .sdo.DataGraph[])
	 */
	@Override
	public Map<TableWriter, List<Row>> collectChanges(DataGraph[] dataGraphs)
			throws IOException, IllegalAccessException {
		Map<TableWriter, List<Row>> mutations = new HashMap<TableWriter, List<Row>>();

		boolean hasChanges = false;
		for (DataGraph dataGraph : dataGraphs) {
			PlasmaChangeSummary changeSummary = (PlasmaChangeSummary) dataGraph
					.getChangeSummary();
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

		for (DataGraph dataGraph : dataGraphs) {
			PlasmaChangeSummary changeSummary = (PlasmaChangeSummary) dataGraph
					.getChangeSummary();
			for (DataObject changed : changeSummary.getChangedDataObjects())
				this.checkConcurrency(dataGraph, (PlasmaDataObject) changed);
		}

		List<DistributedWriter> graphWriters = new ArrayList<DistributedWriter>();
		for (DataGraph dataGraph : dataGraphs) {
			PlasmaChangeSummary changeSummary = (PlasmaChangeSummary) dataGraph
					.getChangeSummary();

			PlasmaDataObject[] created = sortCreated(changeSummary);

			ModifiedObjectCollector modified = new ModifiedObjectCollector(
					dataGraph);
			DeletedObjectCollector deleted = new DeletedObjectCollector(
					dataGraph);

			TableWriterCollector collector = new TableWriterCollector(
					dataGraph, created, modified, deleted);

			DistributedWriter graphWriter = new DistributedGraphWriter(
					dataGraph, collector, this.context.getMarshallingContext());
			graphWriters.add(graphWriter);

			this.create(dataGraph, created, graphWriter);
			this.modify(dataGraph, modified, graphWriter);
			this.delete(dataGraph, deleted, graphWriter);

		}

		for (DistributedWriter graphWriter : graphWriters) {

			for (TableWriter tableWriter : graphWriter.getTableWriters()) {
				List<Row> rowMutations = mutations.get(tableWriter);
				if (rowMutations == null) {
					rowMutations = new ArrayList<Row>();
					mutations.put(tableWriter, rowMutations);
				}

				for (RowWriter rowWriter : tableWriter.getAllRowWriters()) {
					if (log.isDebugEnabled())
						log.debug("commiting data object: "
								+ rowWriter.getRootDataObject().toString());
					addRemoveStateColumns(tableWriter, rowWriter);
					collectRowMutations(tableWriter, rowWriter, rowMutations);
				}
			}
		}

		return mutations;
	}

	private void modify(DataGraph dataGraph, ModifiedObjectCollector modified,
			DistributedWriter graphWriter) throws IllegalAccessException,
			IOException {
		for (PlasmaDataObject dataObject : modified.getResult()) {
			RowWriter rowWriter = graphWriter.getRowWriter(dataObject);
			TableWriter tableWriter = rowWriter.getTableWriter();
			if (log.isDebugEnabled())
				log.debug("validating modifications: "
						+ dataObject.getType().getURI() + "#"
						+ dataObject.getType().getName());
			this.validateModifications(dataGraph, dataObject, rowWriter);

			if (log.isDebugEnabled())
				log.debug("modifying: " + dataObject.getType().getURI() + "#"
						+ dataObject.getType().getName());
			this.update.collect(dataGraph, dataObject, graphWriter,
					tableWriter, rowWriter);
		}
	}

	private void delete(DataGraph dataGraph, DeletedObjectCollector deleted,
			DistributedWriter graphWriter) throws IllegalAccessException,
			IOException {
		for (PlasmaDataObject dataObject : deleted.getResult()) {

			RowWriter rowWriter = graphWriter.getRowWriter(dataObject);
			TableWriter tableWriter = rowWriter.getTableWriter();
			if (log.isDebugEnabled())
				log.debug("deleting: " + dataObject.getType().getURI() + "#"
						+ dataObject.getType().getName());

			this.delete.collect(dataGraph, dataObject, graphWriter,
					tableWriter, rowWriter);
			// rowWriter.getGraphState().removeSequence(dataObject);
		}
	}

	private void create(DataGraph dataGraph, PlasmaDataObject[] created,
			DistributedWriter graphWriter) throws IOException,
			IllegalAccessException {

		for (PlasmaDataObject dataObject : created) {
			RowWriter rowWriter = graphWriter.getRowWriter(dataObject);
			TableWriter tableWriter = rowWriter.getTableWriter();
			if (log.isDebugEnabled())
				log.debug("creating: " + dataObject.getType().getURI() + "#"
						+ dataObject.getType().getName());
			this.create.collect(dataGraph, dataObject, graphWriter,
					tableWriter, rowWriter);
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

	private void collectRowMutations(TableWriter tableWriter,
			RowWriter rowWriter, List<Row> rowMutations) throws IOException {
		rowMutations.addAll(rowWriter.getWriteOperations());
	}

	private void addRemoveStateColumns(TableWriter tableWriter,
			RowWriter rowWriter) throws IOException {
		byte[] fam = tableWriter.getTableConfig()
				.getDataColumnFamilyNameBytes();
		String rootUUID = ((PlasmaDataObject) rowWriter.getRootDataObject())
				.getUUIDAsString();
		if (rowWriter.isRootCreated()) {
			// rowWriter.getRow().addColumn(tableWriter.getTableConfig().getDataColumnFamilyNameBytes(),
			// GraphMetaField.__RU__.asBytes(),
			// Bytes.toBytes(rootUUID));
			// rowWriter.getRow().addColumn(tableWriter.getTableConfig().getDataColumnFamilyNameBytes(),
			// GraphMetaField.__RT__.asBytes(),
			// rowWriter.encodeRootType());
			rowWriter.getRow().addColumn(
					fam,
					GraphMetaKey.TIMESTAMP.codeAsBytes(),
					Bytes.toBytes(String.valueOf(this.snapshotMap
							.getSnapshotNannoTime())));

			if (tableWriter.getTableConfig().tombstoneRowsOverwriteable()) {
				rowWriter.getRowDelete().addColumns( // deletes all versions
						fam, GraphMetaKey.TOMBSTONE.codeAsBytes());
			}
		}

		if (!rowWriter.isRootDeleted()) {
			// String xml = rowWriter.getSequenceMapping().marshalAsString();
			byte[] bytes = rowWriter.getSequenceMapping().marshal();
			if (rowWriter.getSequenceMapping().isUpdated()) {
				rowWriter.getRow().addColumn(fam,
						GraphMetaKey.SEQUENCE_MAPPING.codeAsBytes(), bytes);
			}
		} else { // root is deleted
			if (log.isDebugEnabled())
				log.debug("processing deleted root (" + rootUUID + ")");
			if (tableWriter.getTableConfig().tombstoneRows()) {
				// since this is the root, and someone could be pointing at us,
				// add a tombstone column
				if (log.isDebugEnabled())
					log.debug("adding toumbstone for root "
							+ rowWriter.getRootDataObject().toString());
				rowWriter.getRow().addColumn(
						fam,
						GraphMetaKey.TOMBSTONE.codeAsBytes(),
						Bytes.toBytes(this.snapshotMap.getSnapshotDate()
								.getTime()));
				rowWriter.getRowDelete().addColumns( // deletes all version
						fam, GraphMetaKey.SEQUENCE_MAPPING.codeAsBytes());
			}
		}
	}

	private boolean parentLinksDeleted(PlasmaChangeSummary changeSummary,
			DataObject root) {
		boolean allDeleted = true;
		PlasmaType rootType = (PlasmaType) root.getType();
		List<String> uris = PlasmaRepository.getInstance()
				.getAllNamespaceUris();
		for (String uri : uris) {
			List<Classifier> classifiers = PlasmaRepository.getInstance()
					.getClassifiers(uri);
			for (Classifier c : classifiers) {
				if (c instanceof Class_) {
					((Class_) rootType.getClassifier()).isRelation(
							((Class_) c), AssociationPath.singular_binary);
				}
			}
		}

		return allDeleted;
	}

	/**
	 * Determines if all data object linked to every property where the root
	 * type is its source have been deleted
	 * 
	 * @param changeSummary
	 *            the change summary
	 * @param root
	 *            the root data object
	 * @return whether all data object linked to every property where the root
	 *         type is its source have been deleted
	 */
	private boolean childLinksDeleted(PlasmaChangeSummary changeSummary,
			DataObject root) {
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
					log.warn("potentially incomplete delete graph detected for type, "
							+ rootProp.getType()
							+ " within root graph type "
							+ rootType + " - ");
				}
			}
		}
		return allDeleted;
	}

}
