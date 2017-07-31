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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.state.StateMarshalingContext;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;
import commonj.sdo.Type;

/**
 * Encapsulates one or more graph table writer components for federation across
 * multiple physical tables and/or physical table rows. Maps physical configured
 * table names to respective table writers. In most usage scenarios, a "root"
 * table writer is typically added initially, then other writers are
 * incrementally added as association target types are detected and found
 * configured as graph roots within another distinct table context.
 * <p>
 * Acts as a container for one or more {@link TableWriter} elements
 * encapsulating a set of component table write operations for federation across
 * multiple tables, or a single table in the most simple (degenerate) case.
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.GraphTableWriter
 * @see org.cloudgraph.state.GraphTable
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class DistributedGraphWriter extends WriterSupport
		implements
			DistributedWriter {

	private static Log log = LogFactory.getLog(DistributedGraphWriter.class);
	private TableWriter rootWriter;
	private Map<String, TableWriter> tableWriterMap = new HashMap<String, TableWriter>();
	/** maps table writers to graph-root types */
	private Map<TableWriter, List<Type>> tableWriterTypeMap = new HashMap<TableWriter, List<Type>>();
	private StateMarshalingContext marshallingContext;

	@SuppressWarnings("unused")
	private DistributedGraphWriter() {
	}

	public DistributedGraphWriter(DataGraph dataGraph,
			TableWriterCollector collector,
			StateMarshalingContext marshallingContext) throws IOException {
		this.marshallingContext = marshallingContext;
		this.rootWriter = collector.getRootTableWriter();
		for (TableWriter tableWriter : collector.getTableWriters()) {
			if (log.isDebugEnabled())
				log.debug("added table writer, "
						+ tableWriter.getTableConfig().getName());
			tableWriter.setDistributedWriter(this);
			tableWriterMap.put(tableWriter.getTableConfig().getName(),
					tableWriter);
			List<Type> list = new ArrayList<Type>();
			for (RowWriter rowWriter : tableWriter.getAllRowWriters()) {
				if (!list.contains(rowWriter.getRootType())) {
					list.add(rowWriter.getRootType());
					if (log.isDebugEnabled())
						log.debug("added row writer, "
								+ rowWriter.getRootDataObject());
				}
			}
			tableWriterTypeMap.put(tableWriter, list);
		}
		this.rowWriterMap = collector.getRowWriterMap();
	}

	/**
	 * Returns the table writer for the given configured table name, or null of
	 * not exists.
	 * 
	 * @param tableName
	 *            the name of the configured table.
	 * @return the table writer for the given configured table name, or null of
	 *         not exists.
	 */
	@Override
	public TableWriter getTableWriter(String tableName) {
		return this.tableWriterMap.get(tableName);
	}

	/**
	 * Adds the given table writer to the container
	 * 
	 * @param writer
	 *            the table writer.
	 */
	@Override
	public void addTableWriter(TableWriter writer) {
		String name = writer.getTableConfig().getName();
		if (this.tableWriterMap.get(name) != null)
			throw new OperationException("table writer for '" + name
					+ "' already exists");
		this.tableWriterMap.put(name, writer);
	}

	/**
	 * Returns the count of table writers for this container.
	 * 
	 * @return the count of table writers for this container
	 */
	@Override
	public int getTableWriterCount() {
		return this.tableWriterMap.size();
	}

	/**
	 * Returns all table writers for the this container
	 * 
	 * @return all table writers for the this container
	 */
	public List<TableWriter> getTableWriters() {
		List<TableWriter> result = new ArrayList<TableWriter>();
		result.addAll(this.tableWriterMap.values());
		return result;
	}

	/**
	 * Returns the table writer associated with the data graph root.
	 * 
	 * @return the table writer associated with the data graph root.
	 */
	public TableWriter getRootTableWriter() {
		return this.rootWriter;
	}

	/**
	 * Sets the table writer associated with the data graph root.
	 * 
	 * @param writer
	 *            the table writer
	 */
	public void setRootTableWriter(TableWriter writer) {
		this.rootWriter = writer;
		this.tableWriterMap.put(rootWriter.getTableConfig().getName(),
				rootWriter);
	}

	/**
	 * Returns the row writer associated with the given data object
	 * 
	 * @param dataObject
	 *            the data object
	 * @return the row writer associated with the given data object
	 * @throws IllegalArgumentException
	 *             if the given data object is not associated with any row
	 *             writer.
	 */
	@Override
	public RowWriter getRowWriter(DataObject dataObject) {
		RowWriter result = rowWriterMap.get(dataObject);
		if (result == null)
			throw new IllegalArgumentException("the given data object of type "
					+ dataObject.getType().getURI() + "#"
					+ dataObject.getType().getName()
					+ " is not associated with any row writer");
		return result;
	}

	/**
	 * Returns the row writer associated with the given data object or null if
	 * no row writer is associated.
	 * 
	 * @param dataObject
	 *            the data object
	 * @return the row writer associated with the given data object or null if
	 *         no row writer is associated.
	 */
	@Override
	public RowWriter findRowWriter(DataObject dataObject) {
		return rowWriterMap.get(dataObject);
	}

	/**
	 * Creates and returns a new row writer associated with the given data
	 * object.
	 * 
	 * @param dataObject
	 *            the data object
	 * @return a new row writer associated with the given data object.
	 * @throws IOException
	 * @throws IllegalArgumentException
	 *             if the given data object is already associated with a row
	 *             writer.
	 */
	@Override
	public RowWriter createRowWriter(DataObject dataObject) throws IOException {
		PlasmaType type = (PlasmaType) dataObject.getType();
		TableConfig table = CloudGraphConfig.getInstance().findTable(type);
		RowWriter rowWriter = this.rowWriterMap.get(dataObject);
		if (rowWriter != null)
			throw new IllegalArgumentException("the given data object "
					+ dataObject.toString()
					+ " is already associated with a row writer");

		if (table == null) {
			if (log.isDebugEnabled())
				log.debug("creating unbound writer for: " + dataObject);

			rowWriter = getContainerRowWriter(dataObject);
			if (log.isDebugEnabled())
				log.debug("associating " + type.toString() + " with table '"
						+ rowWriter.getTableWriter().getTableConfig().getName()
						+ "'");
			rowWriter.addDataObject(dataObject);
			this.rowWriterMap.put(dataObject, rowWriter);
		} else {
			// a table is configured with this type as root
			TableWriter tableWriter = (TableWriter) tableWriterMap.get(table
					.getName());
			if (tableWriter == null) {
				tableWriter = new GraphTableWriter(table, this);
				rowWriter = createRowWriter(tableWriter, dataObject);
				tableWriter = rowWriter.getTableWriter();
				tableWriterMap.put(tableWriter.getTableConfig().getName(),
						tableWriter);
			} else { // just add a row writer to existing table writer
				rowWriter = this.addRowWriter(dataObject, tableWriter);
			}
			if (log.isDebugEnabled())
				log.debug("associating (root) "
						+ dataObject.getType().toString() + " with table '"
						+ rowWriter.getTableWriter().getTableConfig().getName()
						+ "'");

			this.rowWriterMap.put(dataObject, rowWriter);

		}
		return rowWriter;
	}

	/**
	 * Returns true if only one table operation exists with only one associated
	 * (root) type for this operation.
	 * 
	 * @return true if only one table operation exists with only one associated
	 *         (root) type for this operation.
	 */
	public boolean hasSingleRootType() {
		if (this.getTableWriterCount() == 1
				&& this.getTypes(this.rootWriter).size() == 1) {
			return true;
		} else
			return false;
	}

	/**
	 * Returns a list of types associated with the given table operation.
	 * 
	 * @param reader
	 *            the table operation
	 * @return a list of types associated with the given table operation.
	 */
	@Override
	public List<Type> getTypes(TableWriter operation) {
		return this.tableWriterTypeMap.get(operation);
	}

	@Override
	public StateMarshalingContext getMarshallingContext() {
		return this.marshallingContext;
	}
}