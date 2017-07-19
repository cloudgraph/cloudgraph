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
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.cloudgraph.hbase.key.StatefullColumnKeyFactory;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataObject;

/**
 * Provides access to the operational, configuration and other state information
 * required for write operations on a single graph row.
 * <p>
 * Acts as a single component within a {@link TableWriter} container
 * and encapsulates the HBase client <a target="#" href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html">Put</a> and
 * <a target="#" href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Delete.html">Delete</a> 
 * operations for use in write operations across multiple logical
 * entities within a graph row. 
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.TableWriter
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface RowWriter extends RowOperation {
	
	/**
	 * Returns the row put mutation.
	 * @return the row put mutation.
	 */
	public Put getRow();

	/**
	 * Creates a new row delete mutation, is not exists.
	 */
	public void deleteRow();

	/**
	 * Returns the existing (or creates a new) row delete mutation.
	 * @return the existing (or creates a new) row delete mutation.
	 */
	public Delete getRowDelete();

	/**
	 * Returns whether there is an existing row delete mutation.
	 * @return whether there is an existing row delete mutation.
	 */
	public boolean hasRowDelete();
	
	
	/**
	 * Return the write operations for a row. 
	 * @return the write operations for a row.
	 */
	public List<Row> getWriteOperations();

	/**
	 * Returns a single column value for this row given a context
	 * data object and property. Uses a statefull column key factory
	 * to generate a column key based on the given context data object 
	 * and property.
	 * @param dataObject the context data object
	 * @param property the context property
	 * @return the column value bytes
	 * @throws IOException
	 * 
	 * @see StatefullColumnKeyFactory
	 */
	public byte[] fetchColumnValue(PlasmaDataObject dataObject, 
			PlasmaProperty property) throws IOException;

		/**
	 * Returns the container for this writer. 
	 * @return the container for this writer.
	 */
	public TableWriter getTableWriter();
	
	/**
	 * Returns whether the root data object for this writer
	 * is created. 
	 * @return whether the root data object for this writer
	 * is created.
	 */
	public boolean isRootCreated();
	
	/**
	 * Returns whether the root data object for this writer
	 * is deleted. 
	 * @return whether the root data object for this writer
	 * is deleted.
	 */
	public boolean isRootDeleted();
	
	public long newSequence(PlasmaDataObject dataObject) throws IOException;
	
	public void writeRowEntityMetaData(PlasmaDataObject dataObject, long sequence) throws IOException;
	public void deleteRowEntityMetaData(PlasmaDataObject dataObject, long sequence) throws IOException;
	
	public void writeRowData(PlasmaDataObject dataObject, long sequence, PlasmaProperty property, byte[] value) throws IOException;
	public void deleteRowData(PlasmaDataObject dataObject, long sequence, PlasmaProperty property) throws IOException;
	
	/**
	 * Returns an existing or new edge writer for the given data object and source edge property
	 * @param dataObject the data object
	 * @param property the source edge property
	 * @return an existing or new edge writer for the given data object and source edge property
	 * @throws IOException
	 */
	//public EdgeWriter getEdgeWriter(PlasmaDataObject dataObject, PlasmaProperty property) throws IOException;
	
	/**
	 * Returns an existing or new edge writer for the given data object, sequence and source edge property
	 * @param dataObject the data object
	 * @param property the source edge property
	 * @param sequence the sequence for the given data object type, unique within the graph/row
	 * @return an existing or new edge writer for the given data object, sequence and source edge property
	 * @throws IOException
	 */
	public EdgeWriter getEdgeWriter(PlasmaDataObject dataObject, PlasmaProperty property, long sequence) throws IOException;

	/**
	 * Returns the edge writer for the source edge or null if not exists.  
	 * @param dataObject the data object
	 * @param sequence the sequence
	 * @return the edge writer for the source edge or null if not exists.
	 * @throws IOException
	 */
	//public EdgeWriter getEdgeWriter(PlasmaDataObject dataObject, long sequence) throws IOException;

	/**
	 * 
	 * @param dataObject
	 * @param sequence
	 * @param type
	 */
	public void addSequence(DataObject dataObject, long sequence);	
	public boolean containsSequence(DataObject dataObject);
	public long getSequence(DataObject dataObject);
	
	public byte[] encodeRootType();
	public byte[] encodeType(PlasmaType type);	
}
