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
 * Fields used to describe a set or collection of edges used to find and de-reference
 * local or remote entities. 
 * @author Scott Cinnamond
 * @since 1.0.0
 */
public enum EdgeMetaField implements MetaField {
	
	/**
	 * The timestamp for an edge/collection indicating the last modified
	 * date for the edge, the value being a string representation of a long integer.  
	 */
	_ETS_(),

	/**
	 * This field specifies the base entity type for the (abstract) base class for the reference 
	 * collection. The value associated with this field is a qualified type identifier.  
	 */
	_BTP_(),
	/**
	 * The default entity subtype for elements within an edge or reference collection. The default sybtype
	 * is only created if the actual entity type differs from the base entity type. If valued it does not
	 * require all future entities within the collection to be instances of the sybtype. 
	 */
	_STP_(),
	/**
	 * This field specifies the qualified physical table name for the reference 
	 * collection, only used when the targets are not local to 
	 * the current graph row.
	 * FIXME: could potentially be eliminated via dynamic store lookups   
	 */
	_TBL_(),
	/**
	 * This field specifies the path to the collection within a remote graph. This path is used 
	 * to locate target entities anywhere within a non-local target graph and is essential 
	 * for reference cleanup during delete operations. The value associated with this field 
	 * is a path composed of physical property names from the graph root to the target collection. 
	 */
	_PTH_(),
	/**
	 * This field specifies the count of entities associated with the collection. 
	 */
	_CNT_(),
	/**
	 * This field specifies the entity sequence numbers for the target entities contained in 
	 * the target collection. The sequence numbers within the value for this field are added 
	 * during insert operations and removed during delete operations. All such reference 
	 * metadata mutations must be associated and kept in sync with mutations on the 
	 * actual target entities. 
	 */
	_SQS_(),
	
	/**
	 * This field specifies the row keys associated with the target entities for the reference 
	 * collection, where the targets are not part of the local graph, but are found 
	 * in another row or table.
	 */
	_KYS_();
	
	byte[] bytesName;
	private EdgeMetaField() {
		this.bytesName = this.name().getBytes(
			Charset.forName( CoreConstants.UTF8_ENCODING ));
	}
	
    public byte[] asBytes() {
    	return this.bytesName;
    }

	@Override
	public String asString() {
		return this.name();
	}
}
