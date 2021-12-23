/**
 * Copyright 2017 TerraMeta Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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