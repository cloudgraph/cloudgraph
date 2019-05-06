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

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.sdo.core.CoreConstants;

/**
 * Encapsulates logic related to access of HTable specific configuration
 * information.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public abstract class TableMapping {
  private static Log log = LogFactory.getLog(TableMapping.class);
  /** Apache HBase namespace table-name delimiter */
  static final String TABLE_LOGICAL_NAME_DELIM = "/";
  /** MAPRDB table path delimiter */
  static final String TABLE_PHYSICAL_NAME_DELIM = "/";
  static final String TABLE_PHYSICAL_NAMESPACE_DELIM = "_";
  static final String TABLE_NAME_DEFAULT_NAMESPACE = "default";

  protected Table table;
  private MappingConfiguration config;
  private Charset charset;
  private Map<String, Property> propertyNameToPropertyMap = new HashMap<String, Property>();

  @SuppressWarnings("unused")
  private TableMapping() {
  }

  public TableMapping(Table table, MappingConfiguration config) {
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

  private void validate() {
    String tableName = this.table.getName();
    if (tableName == null || tableName.trim().length() == 0)
      throw new IllegalStateException("table name cannot be null or empty");
    if (tableName.contains(":"))
      throw new IllegalStateException("table name cannot contain ':' char");
  }

  /**
   * Returns the configuration for the HTable.
   * 
   * @return the configuration for the HTable.
   */
  public Table getTable() {
    return table;
  }

  public MappingConfiguration getMappingConfiguration() {
    return config;
  }

  /**
   * Returns the table name for this table configuration.
   * 
   * @return the table name for this table configuration.
   */
  // public String getName() {
  // String name = this.table.getName();
  // String prefix = this.maprdbTablePathPrefix();
  // if (prefix != null) {
  // if (!prefix.endsWith(TABLE_PATH_DELIM) &&
  // !name.startsWith(TABLE_PATH_DELIM))
  // prefix = prefix + TABLE_PATH_DELIM;
  // name = prefix + name;
  // }
  // return name;
  // }

  /**
   * Returns the table namespace for this table configuration.
   * 
   * @return the table namespace for this table configuration.
   */
  // public String getNamespace() {
  // return this.table.getNamespace();
  // }

  public abstract StoreMappingContext getMappingContext();

  protected String qualifiedPhysicalName;

  public abstract String getQualifiedPhysicalName();

  protected String relativePhysicalName;

  public abstract String getRelativePhysicalName();

  protected String qualifiedLogicalName;

  public abstract String getQualifiedLogicalName();

  protected String qualifiedPhysicalNamespace;

  public abstract String getQualifiedPhysicalNamespace();

  public static String qualifiedLogicalNameFor(String namespace, String tableName,
      StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    if (context != null && context.hasMaprdbVolumePath()
        && !tableName.startsWith(context.getMaprdbVolumePath())) {
      name.append(context.getMaprdbVolumePath());
      name.append(TABLE_LOGICAL_NAME_DELIM);
    }
    if (namespace != null) {
      name.append(namespace);
      name.append(TABLE_LOGICAL_NAME_DELIM);
    }
    name.append(tableName);
    return name.toString();
  }

  public static String qualifiedLogicalNameFor(QName typeName, StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    if (context != null && context.hasMaprdbVolumePath()) {
      name.append(context.getMaprdbVolumePath());
      name.append(TABLE_LOGICAL_NAME_DELIM);
    }
    name.append(typeName.toString());
    return name.toString();
  }

  public static String qualifiedPhysicalNameFor(String namespace, String tableName,
      StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    String rootPath = StoreMapping.getInstance().maprdbTablePathPrefix();
    if (rootPath != null) {
      name.append(rootPath);
      name.append(TABLE_PHYSICAL_NAME_DELIM);
    }
    if (context != null && context.hasMaprdbVolumePath()
        && !tableName.startsWith(context.getMaprdbVolumePath())) {
      name.append(context.getMaprdbVolumePath());
      name.append(TABLE_PHYSICAL_NAME_DELIM);
    }
    if (namespace != null) {
      name.append(namespace);
      name.append(TABLE_PHYSICAL_NAME_DELIM);
    }
    name.append(tableName);
    return name.toString();
  }

  public static String relativePhysicalNameFor(String namespace, String tableName,
      StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    if (context != null && context.hasMaprdbVolumePath()
        && !tableName.startsWith(context.getMaprdbVolumePath())) {
      name.append(context.getMaprdbVolumePath());
      name.append(TABLE_PHYSICAL_NAME_DELIM);
    }
    if (namespace != null) {
      name.append(namespace);
      name.append(TABLE_PHYSICAL_NAME_DELIM);
    }
    name.append(tableName);
    return name.toString();
  }

  public static String qualifiedPhysicalNamespaceFor(String namespace, String tableName,
      StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    String rootPath = StoreMapping.getInstance().maprdbTablePathPrefix();
    if (rootPath == null) {
      if (context != null && context.hasMaprdbVolumePath()
          && !namespace.startsWith(context.getMaprdbVolumePath())) {
        name.append(context.getMaprdbVolumePath());
        name.append(TABLE_PHYSICAL_NAME_DELIM);
      }

      name.append(namespace);
    }
    return name.toString();
  }

  public String getDataColumnFamilyName() {
    return this.table.getDataColumnFamilyName();
  }

  private byte[] columnFamilyNameBytes;

  public byte[] getDataColumnFamilyNameBytes() {
    if (this.columnFamilyNameBytes == null) {
      this.columnFamilyNameBytes = this.table.getDataColumnFamilyName().getBytes(this.charset);
    }
    return this.columnFamilyNameBytes;
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

  private String maprdbVolumePathPrefixVar = null;

  public String maprdbVolumePathPrefix() {
    if (maprdbVolumePathPrefixVar == null) {
      maprdbVolumePathPrefixVar = getTablePropertyString(
          ConfigurationProperty.CLOUDGRAPH___MAPRDB___VOLUME___PATH___PREFIX,
          this.table.getMaprdbVolumePathPrefix(), null);
    }
    return this.maprdbVolumePathPrefixVar;
  }

  private Boolean optimisticConcurrencyVar = null;

  public boolean optimisticConcurrency() {
    if (optimisticConcurrencyVar == null) {
      optimisticConcurrencyVar = getTablePropertyBoolean(
          ConfigurationProperty.CLOUDGRAPH___CONCURRENCY___OPTIMISTIC___ENABLED,
          this.table.isOptimisticConcurrency(), true);
    }
    return this.optimisticConcurrencyVar.booleanValue();
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