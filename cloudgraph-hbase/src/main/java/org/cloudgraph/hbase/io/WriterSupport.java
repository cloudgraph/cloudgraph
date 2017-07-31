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
package org.cloudgraph.hbase.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.hbase.key.CompositeRowKeyFactory;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreDataObject;

import commonj.sdo.DataObject;

public abstract class WriterSupport {
	private static Log log = LogFactory.getLog(WriterSupport.class);
	protected Map<DataObject, RowWriter> rowWriterMap = new HashMap<DataObject, RowWriter>();

	protected WriterSupport() {

	}

	/**
	 * Finds and return the row writer associated with the container for the
	 * given target data object
	 * 
	 * @param target
	 *            the target data object
	 * @return the row writer associated with the container for the given target
	 *         data object
	 * @throws OperationException
	 *             if no row writer found
	 */
	protected RowWriter getContainerRowWriter(DataObject target) {
		RowWriter result = findContainerRowWriter(target);
		if (result == null)
			throw new OperationException(
					"no row writer associated with data object, "
							+ String.valueOf(target)
							+ ", or its containment ancestry");
		return result;
	}

	/**
	 * Finds and return the row writer associated with the given target data
	 * object
	 * 
	 * @param target
	 *            the target data object
	 * @return the row writer associated with the given target data object, or
	 *         null if no writer found
	 */
	protected RowWriter findContainerRowWriter(DataObject target) {
		RowWriter result = null;
		DataObject container = target.getContainer();
		while (container != null) {
			if (log.isDebugEnabled())
				log.debug("checking " + container + " container for " + target);
			result = this.rowWriterMap.get(container);
			if (result != null)
				return result;
			container = container.getContainer();
		}
		return null;
	}

	/**
	 * Traverses the container links for the given data object returning the
	 * ancestry as an ordered list.
	 * 
	 * @param target
	 * @return
	 */
	protected List<DataObject> getContainerAncestry(DataObject target) {
		List<DataObject> result = new ArrayList<DataObject>();
		DataObject source = target.getContainer();
		while (source != null) {
			result.add(source);
			source = source.getContainer();
		}
		return result;
	}

	/**
	 * Creates and returns a new row writer for the given (row root) data
	 * object. If an existing row key is set into the instance property
	 * collection for the (row root) data object, the existing row key is used,
	 * otherwise a row key is generated. For objects flagged as deleted in the
	 * SDO change summary, re-generating a row key may not be possible, as for
	 * deleted operations SDO requires the removal of all properties (other than
	 * read-only) for all deleted data objects. If such removed properties are
	 * required for a row key, this makes re-generation for the row key
	 * impossible, and the key generator will throw an error.
	 * 
	 * @param tableWriter
	 *            the table writer
	 * @param target
	 *            the target (row root) data object
	 * @return the new row writer
	 * @throws IOException
	 */
	protected RowWriter createRowWriter(TableWriter tableWriter,
			DataObject target) throws IOException {
		CoreDataObject coreObject = (CoreDataObject) target;
		byte[] rowKey = (byte[]) coreObject.getValueObject().get(
				CloudGraphConstants.ROW_KEY);
		if (rowKey == null) {
			CompositeRowKeyFactory rowKeyGen = new CompositeRowKeyFactory(
					(PlasmaType) target.getType());
			rowKey = rowKeyGen.createRowKeyBytes(target);
			coreObject.getValueObject()
					.put(CloudGraphConstants.ROW_KEY, rowKey);
			if (target.getDataGraph().getRootObject().equals(target)) // graph
																		// root
																		// object
				((PlasmaDataGraph) target.getDataGraph()).setId(rowKey); // FIXME:
																			// snapshot
																			// map
																			// for
																			// this?
		}
		RowWriter rowWriter = this.createRowWriter(tableWriter, target, rowKey);

		return rowWriter;
	}

	/**
	 * Creates and returns a new row writer for the given (row root) data
	 * object.
	 * 
	 * @param tableContext
	 *            the table context
	 * @param dataObject
	 *            the (row root) data object
	 * @param rowKey
	 *            the existing row key
	 * @return the new row writer
	 * @throws IOException
	 */
	protected RowWriter createRowWriter(TableWriter tableContext,
			DataObject dataObject, byte[] rowKey) throws IOException {
		RowWriter rowContext = new GraphRowWriter(rowKey, dataObject,
				tableContext);

		UUID uuid = ((PlasmaDataObject) dataObject).getUUID();
		tableContext.addRowWriter(uuid, rowContext);

		return rowContext;
	}

	/**
	 * Creates adds and returns a new row writer for the given (row root) data
	 * object. If an existing row key is set into the instance property
	 * collection for the (row root) data object, the existing row key is used,
	 * otherwise a row key is generated. For objects flagged as deleted in the
	 * SDO change summary, re-generating a row key may not be possible, as for
	 * deleted operations SDO requires the removal of all properties (other than
	 * read-only) for all deleted data objects. If such removed properties are
	 * required for a row key, this makes re-generation for the row key
	 * impossible, and the key generator will throw an error.
	 * 
	 * @param dataObject
	 *            the target (row root) data object
	 * @param tableContext
	 *            the table context
	 * @return the new row writer
	 * @throws IOException
	 */
	protected RowWriter addRowWriter(DataObject dataObject,
			TableWriter tableContext) throws IOException {
		byte[] rowKey = null;
		CoreDataObject coreObject = (CoreDataObject) dataObject;
		rowKey = (byte[]) coreObject.getValueObject().get(
				CloudGraphConstants.ROW_KEY);
		if (rowKey == null) {
			CompositeRowKeyFactory rowKeyGen = new CompositeRowKeyFactory(
					(PlasmaType) dataObject.getType());
			rowKey = rowKeyGen.createRowKeyBytes(dataObject);
			coreObject.getValueObject()
					.put(CloudGraphConstants.ROW_KEY, rowKey);
		}
		RowWriter rowContext = new GraphRowWriter(rowKey, dataObject,
				tableContext);
		UUID uuid = ((PlasmaDataObject) dataObject).getUUID();
		tableContext.addRowWriter(uuid, rowContext);
		return rowContext;
	}
}
