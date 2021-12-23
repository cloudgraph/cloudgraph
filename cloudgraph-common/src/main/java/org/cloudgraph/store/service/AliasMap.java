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
package org.cloudgraph.store.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.plasma.sdo.PlasmaType;

/**
 * Simple collection managing SQL table aliases
 */
public class AliasMap {

  private Map<PlasmaType, String> map = null;

  @SuppressWarnings("unused")
  private AliasMap() {
  }

  public AliasMap(PlasmaType root) {
    this.map = new HashMap<PlasmaType, String>();
    this.map.put(root, "t0");
  }

  /**
   * Returns the table alias for the given type
   * 
   * @param type
   *          the type
   * @return the alias
   */
  public String getAlias(PlasmaType type) {
    return this.map.get(type);
  }

  /**
   * Return the types which are keys for this map.
   * 
   * @return the type keys
   */
  public Iterator<PlasmaType> getTypes() {
    return this.map.keySet().iterator();
  }

  /**
   * Returs the alias names for this map
   * 
   * @return the alias names
   */
  public Collection<String> getAliases() {
    return this.map.values();
  }

  /**
   * Adds and returns the table alias for the given type, or existing alias of
   * already mapped
   * 
   * @param type
   *          the type
   * @return the new alias or existing alias of already mapped
   */
  public String addAlias(PlasmaType type) {
    String existing = this.map.get(type);
    if (existing == null) {
      String alias = "t" + String.valueOf(this.map.size());
      this.map.put(type, alias);
      return alias;
    }
    return existing;
  }

}
