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
 * Enumeration based qualifier or code-set used to describe an "entity" or
 * instance of some type within a graph.
 * <p>
 * </p>
 * The enumeration name is referenced directly in source code, and the
 * underlying code is used as the actual physical column qualifier. The allows
 * the source to remain more readable while allowing more terse physical
 * qualifiers or column keys.
 * 
 * @author Scott Cinnamond
 * @since 1.0.0
 */
public enum EntityMetaKey implements MetaKey {
	/**
	 * The timestamp for an entire entity within a graph/row indicating the last
	 * modified date for the entity, the value being a string representation of
	 * a long integer.
	 */
	TIMESTAMP(
			"_TS",
			"timestamp for an entire entity within a graph/row indicating the last modified date for the entity"),
	/**
	 * Represents the uuid of an entity, and is a mandatory field for all
	 * queries. The value associated with this field is a uuid which allows all
	 * assembled data object to be universally unique across sessions and
	 * clients
	 */
	UUID("_UU", "the UUID for an entity"),
	/**
	 * The qualified type name for the entity. Used to dynamically determine the
	 * type/subtype of the entity e.g. when unmarshalled or de-referenced as
	 * part of an edge (collection).
	 */
	TYPE("_TP", "qualified type name for an entity");

	private String code;
	private String description;
	byte[] codeBytes;
	private EntityMetaKey(String code, String description) {
		this.code = code;
		this.description = description;
		this.codeBytes = this.code.getBytes(Charset
				.forName(CoreConstants.UTF8_ENCODING));
	}

	@Override
	public byte[] codeAsBytes() {
		return this.codeBytes;
	}

	@Override
	public String asString() {
		return this.name() + " (" + this.code + ")";
	}

	@Override
	public String code() {
		return this.code;
	}

	@Override
	public String description() {
		return this.description;
	}
}
