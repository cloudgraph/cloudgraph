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

import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.PropertyPair;

import commonj.sdo.Property;

/**
 * Common graph assembler functionality resulting from initial re-factoring and
 * addition of parallel assembly across RDB and Cassandra services.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public interface StatementExecutor {

  public abstract List<List<PropertyPair>> fetch(PlasmaType type, StringBuilder sql);

  public abstract List<List<PropertyPair>> fetch(PlasmaType type, StringBuilder sql,
      Set<Property> props);

  public abstract List<List<PropertyPair>> fetch(PlasmaType type, StringBuilder sql,
      Set<Property> props, Object[] params);

  public abstract Map<String, PropertyPair> fetchRowMap(PlasmaType type, StringBuilder sql);

  public abstract List<PropertyPair> fetchRow(PlasmaType type, StringBuilder sql);

  public abstract void execute(PlasmaType type, StringBuilder sql, Map<String, PropertyPair> values);

  public abstract void executeInsert(PlasmaType type, StringBuilder sql,
      Map<String, PropertyPair> values);

  public List<PropertyPair> executeInsertWithGeneratedKeys(PlasmaType type, StringBuilder sql,
      Map<String, PropertyPair> values);
}