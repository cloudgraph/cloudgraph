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

import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.PropertyPair;

import commonj.sdo.Property;
import commonj.sdo.Type;

/**
 * Common graph assembler functionality resulting from initial re-factoring and
 * addition of parallel assembly across RDB and Cassandra services.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public interface StatementFactory {

  public abstract StringBuilder createSelectConcurrent(PlasmaType type,
      List<PropertyPair> keyValues, int waitSeconds, List<Object> params);

  public abstract StringBuilder createSelect(PlasmaType type, Set<Property> props,
      List<PropertyPair> keyValues, List<Object> params);

  public abstract StringBuilder createSelect(PlasmaType type, Set<Property> props,
      List<PropertyPair> keyValues, FilterAssembler filterAssembler, List<Object> params);

  public abstract StringBuilder createInsert(PlasmaType type, Map<String, PropertyPair> values);

  public abstract StringBuilder createUpdate(PlasmaType type, Map<String, PropertyPair> values);

  public abstract StringBuilder createDelete(PlasmaType type, Map<String, PropertyPair> values);

  public abstract PlasmaProperty getOppositePriKeyProperty(Property targetProperty);

  public abstract boolean hasUpdatableProperties(Map<String, PropertyPair> values);

  public abstract FilterAssembler createFilterAssembler(Where where, Type targetType);

}