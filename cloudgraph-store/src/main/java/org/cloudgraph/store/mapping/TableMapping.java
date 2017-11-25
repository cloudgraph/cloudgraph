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
package org.cloudgraph.store.mapping;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudgraph.store.mapping.ConfigurationProperty;
import org.cloudgraph.store.mapping.Property;
import org.cloudgraph.store.mapping.Table;
import org.plasma.sdo.core.CoreConstants;

/**
 * Encapsulates logic related to access of HTable specific configuration
 * information.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class TableMapping {
  /** Apache HBase namespace table-name delimiter */
  private static final String TABLE_NAME_DELIM = ":";
  /** MAPRDB table path delimiter */
  private static final String TABLE_PATH_DELIM = "/";
  private Table table;
  private Config config;
  private Charset charset;
  private Map<String, Property> propertyNameToPropertyMap = new HashMap<String, Property>();

  @SuppressWarnings("unused")
  private TableMapping() {
  }

  public TableMapping(Table table, Config config) {
    super();
    this.table = table;
    this.config = config;
    this.charset = Charset.forName(CoreConstants.UTF8_ENCODING);
    for (Property prop : table.getProperties())
      propertyNameToPropertyMap.put(prop.getName(), prop);
  }

  public TableMapping(Table table) {
    this(table, StoreMapping.getInstance());
  }

  /**
   * Returns the configuration for the HTable.
   * 
   * @return the configuration for the HTable.
   */
  public Table getTable() {
    return table;
  }

  /**
   * Returns the table name for this table configuration.
   * 
   * @return the table name for this table configuration.
   */
  public String getName() {
    String name = this.table.getName();
    String prefix = this.maprdbTablePathPrefix();
    if (prefix != null) {
      if (!prefix.endsWith(TABLE_PATH_DELIM) && !name.startsWith(TABLE_PATH_DELIM))
        prefix = prefix + TABLE_PATH_DELIM;
      name = prefix + name;
    }
    return name;
  }

  /**
   * Returns the table namespace for this table configuration.
   * 
   * @return the table namespace for this table configuration.
   */
  public String getNamespace() {
    return this.table.getNamespace();
  }

  public String getQualifiedName() {
    return qualifiedNameFor(this.getNamespace(), this.getName());
  }

  public static String qualifiedNameFor(String namespace, String tableName) {
    String qualifiedName = tableName;
    if (namespace != null)
      qualifiedName = namespace + TABLE_NAME_DELIM + qualifiedName;
    return qualifiedName;
  }

  public String getDataColumnFamilyName() {
    return this.table.getDataColumnFamilyName();
  }

  public byte[] getDataColumnFamilyNameBytes() {
    return this.table.getDataColumnFamilyName().getBytes(this.charset);
  }

  /**
   * Returns true if the table has a specific hash algorithm configured.
   * 
   * @return true if the table has a specific hash algorithm configured.
   */
  public boolean hasHashAlgorithm() {
    return this.getTable().getHashAlgorithm() != null;
  }

  public Charset getCharset() {
    return charset;
  }

  public List<Property> getProperties() {
    return table.properties;
  }

  public Property findProperty(String name) {
    return this.propertyNameToPropertyMap.get(name);
  }

  private Boolean uniqueChecksVar = null;

  public boolean uniqueChecks() {
    if (uniqueChecksVar == null) {
      uniqueChecksVar = getTablePropertyBoolean(ConfigurationProperty.CLOUDGRAPH___UNIQUE___CHECKS,
          this.table.isUniqueChecks(), true);
    }
    return this.uniqueChecksVar.booleanValue();
  }

  private Boolean tombstoneRowsVar = null;

  public boolean tombstoneRows() {
    if (tombstoneRowsVar == null) {
      tombstoneRowsVar = getTablePropertyBoolean(
          ConfigurationProperty.CLOUDGRAPH___TOMBSTONE___ROWS, this.table.isTombstoneRows(), true);
    }
    return this.tombstoneRowsVar.booleanValue();
  }

  private Boolean tombstoneRowsOverwriteableVar = null;

  public boolean tombstoneRowsOverwriteable() {
    if (tombstoneRowsOverwriteableVar == null) {
      tombstoneRowsOverwriteableVar = getTablePropertyBoolean(
          ConfigurationProperty.CLOUDGRAPH___TOMBSTONE___ROWS___OVERWRITEABLE,
          this.table.isTombstoneRowsOverwriteable(), false);
    }
    return this.tombstoneRowsOverwriteableVar.booleanValue();
  }

  private String maprdbTablePathPrefixVar = null;

  public String maprdbTablePathPrefix() {
    if (maprdbTablePathPrefixVar == null) {
      maprdbTablePathPrefixVar = getTablePropertyString(
          ConfigurationProperty.CLOUDGRAPH___MAPRDB___TABLE___PATH___PREFIX,
          this.table.getMaprdbTablePathPrefix(), null);
    }
    return this.maprdbTablePathPrefixVar;
  }

  /**
   * Checks for system property, then value changed at table level, then global
   * config level.
   * 
   * @param prop
   *          the config prop
   * @param tableValue
   *          the current table value
   * @param defaultValue
   *          the default for the property.
   * @return the String value
   */
  private String getTablePropertyString(ConfigurationProperty prop, String tableValue,
      String defaultValue) {
    String result;

    String value = this.config.getConfigProperties().getString(prop.value());
    if (value != null) {
      result = value;
    } else {
      if (defaultValue != tableValue) { // default overridden, take the
        // table value
        result = tableValue;
      } else { // check for table level generic prop
        Property tableProp = this.findProperty(prop.value());
        if (tableProp != null) {
          result = tableProp.getValue();
        } else { // check for global config prop
          Property globalProp = this.config.findProperty(prop.value());
          if (globalProp != null) {
            result = globalProp.getValue();
          } else { // otherwise use schema default
            result = defaultValue;
          }
        }
      }
    }
    return result;
  }

  /**
   * Checks for system property, then value changed at table level, then global
   * config level.
   * 
   * @param prop
   *          the config prop
   * @param tableValue
   *          the current table value
   * @param defaultValue
   *          the default for the property.
   * @return the boolean value
   */
  private Boolean getTablePropertyBoolean(ConfigurationProperty prop, boolean tableValue,
      boolean defaultValue) {
    Boolean result;

    String value = this.config.getConfigProperties().getString(prop.value());
    if (value != null) {
      result = Boolean.valueOf(value);
    } else {
      if (defaultValue != tableValue) { // default overridden, take the
        // table value
        result = Boolean.valueOf(tableValue);
      } else { // check for table level generic prop
        Property tableProp = this.findProperty(prop.value());
        if (tableProp != null) {
          result = Boolean.valueOf(tableProp.getValue());
        } else { // check for global config prop
          Property globalProp = this.config.findProperty(prop.value());
          if (globalProp != null) {
            result = Boolean.valueOf(globalProp.getValue());
          } else { // otherwise use schema default
            result = Boolean.valueOf(defaultValue);
          }
        }
      }
    }
    return result;
  }
}