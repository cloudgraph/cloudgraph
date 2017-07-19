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

import java.nio.charset.Charset;

import org.plasma.sdo.core.CoreConstants;

/**
 * Fields used to describe an "entity" or instance of some type within a graph.
 * @author Scott Cinnamond
 * @since 1.0.0
 */
public enum EntityMetaField implements MetaField {
	/**
	 * The timestamp for an entire entity within a graph/row indicating the last modified
	 * date for the entity, the value being a string representation of a long integer.  
	 */
	_NTS_(),
	/**
	 * Represents the uuid of an entity, and is a mandatory field for all queries. The value associated with this 
	 * field is a uuid which allows all assembled data object to be universally unique 
	 * across sessions and clients
	 */
	_UU_(),
	/**
	 * The qualified type name for the entity. Used to dynamically determine the type/subtype of the
	 * entity e.g. when unmarshalled or de-referenced as part of an edge (collection). 
	 */
	_TP_();

	byte[] bytesName;
	private EntityMetaField() {
		this.bytesName = this.name().getBytes(
			Charset.forName( CoreConstants.UTF8_ENCODING ));
	}
	
	@Override
    public byte[] asBytes() {
    	return this.bytesName;
    }
    
	@Override
	public String asString() {
		return this.name();
	}
}
