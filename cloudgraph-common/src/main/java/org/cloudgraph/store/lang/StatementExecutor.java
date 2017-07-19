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
package org.cloudgraph.store.lang;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.PropertyPair;

import commonj.sdo.Property;

/**
 * Common graph assembler functionality resulting from initial re-factoring and addition of parallel 
 * assembly across RDB and Cassandra services. 
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public interface StatementExecutor {

	public abstract List<List<PropertyPair>> fetch(PlasmaType type,
			StringBuilder sql);

	public abstract List<List<PropertyPair>> fetch(PlasmaType type,
			StringBuilder sql, Set<Property> props);

	public abstract List<List<PropertyPair>> fetch(PlasmaType type,
			StringBuilder sql, Set<Property> props, Object[] params);

	public abstract Map<String, PropertyPair> fetchRowMap(PlasmaType type,
			StringBuilder sql);

	public abstract List<PropertyPair> fetchRow(PlasmaType type,
			StringBuilder sql);

	public abstract void execute(PlasmaType type, StringBuilder sql,
			Map<String, PropertyPair> values);

	public abstract void executeInsert(PlasmaType type, StringBuilder sql,
			Map<String, PropertyPair> values);

	public List<PropertyPair> executeInsertWithGeneratedKeys(PlasmaType type, StringBuilder sql, 
			Map<String, PropertyPair> values);
}