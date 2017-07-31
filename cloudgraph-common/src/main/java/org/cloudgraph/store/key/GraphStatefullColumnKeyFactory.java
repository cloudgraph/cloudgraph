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
package org.cloudgraph.store.key;

import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Generates a column key based on the configured CloudGraph column key
 * {@link org.cloudgraph.config.ColumnKeyModel model} for a specific table
 * {@link org.cloudgraph.config.Table configuration}.
 * <p>
 * The initial creation and subsequent re-constitution for query retrieval
 * purposes of both row and column keys in CloudGraph&#8482; is efficient, as it
 * leverages byte array level API in both Java and the current underlying SDO
 * 2.1 implementation, <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a>. Both composite row and
 * column keys are composed in part of structural metadata, and the lightweight
 * metadata API within <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a> contains byte-array level,
 * cached lookup of all basic metadata elements including logical and physical
 * type and property names.
 * </p>
 * 
 * @see org.cloudgraph.config.ColumnKeyModel
 * @see org.cloudgraph.config.Table
 * @author Scott Cinnamond
 * @since 0.5
 */
public interface GraphStatefullColumnKeyFactory extends GraphColumnKeyFactory {

	/**
	 * Generates and returns a column key based on the given arguments as well
	 * as the configured CloudGraph column key
	 * {@link org.cloudgraph.config.ColumnKeyModel model} for a specific table
	 * {@link org.cloudgraph.config.Table configuration}.
	 * 
	 * @param dataObject
	 *            the data object
	 * @param sequenceNum
	 *            the graph row specific sequence number for the type associated
	 *            with the given data object
	 * @param property
	 *            the property
	 * @return the column key bytes
	 */
	public byte[] createColumnKey(PlasmaDataObject dataObject,
			long sequenceNum, PlasmaProperty property);

	/**
	 * Generates and returns a column key based on the given arguments as well
	 * as the configured CloudGraph column key
	 * {@link org.cloudgraph.config.ColumnKeyModel model} for a specific table
	 * {@link org.cloudgraph.config.Table configuration}.
	 * 
	 * @param dataObject
	 *            the data object
	 * @param sequenceNum
	 *            the graph row specific sequence number for the type associated
	 *            with the given data object
	 * @param property
	 *            the property
	 * @param metaField
	 *            the meta field
	 * @return the column key bytes
	 */
	public byte[] createColumnKey(PlasmaDataObject dataObject,
			long sequenceNum, PlasmaProperty property, EdgeMetaKey metaField);

	public byte[] createColumnKey(PlasmaType type, long sequenceNum,
			PlasmaProperty property, EntityMetaKey metaField);

	public byte[] createColumnKey(PlasmaType type, long sequenceNum,
			PlasmaProperty property, EdgeMetaKey metaField);

	public byte[] createColumnKey(PlasmaType type, long sequenceNum,
			EntityMetaKey metaField);
	public byte[] createColumnKey(PlasmaType type, long sequenceNum,
			EdgeMetaKey metaField);

	/**
	 * Generates and returns a column key based on data graph specific sequence
	 * number for a given data object, and using the given metadata property as
	 * well as the configured CloudGraph column key
	 * {@link org.cloudgraph.config.ColumnKeyModel model} for a specific table
	 * {@link org.cloudgraph.config.Table configuration}.
	 * 
	 * @param type
	 *            the metadata type
	 * @param sequenceNum
	 *            the graph row specific sequence number for the given type for
	 *            a data object
	 * @param property
	 *            the property
	 * @return the column key bytes
	 */
	public byte[] createColumnKey(PlasmaType type, long sequenceNum,
			PlasmaProperty property);

}
