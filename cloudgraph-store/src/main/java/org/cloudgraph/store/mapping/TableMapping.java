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

  public static final String TABLE_LOGICAL_NAME_DELIM = "#";

  /**
   * Apache HBase namespace and namespace delimiter. Let other store providers
   * override as needed
   */
  public static final String TABLE_PHYSICAL_NAME_DELIM = "_";

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
    validate();
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
    String tableNamespace = this.table.getNamespace();
    if (tableNamespace == null || tableNamespace.trim().length() == 0)
      throw new IllegalStateException("table namespace cannot be null or empty");
    if (tableNamespace.contains(":"))
      throw new IllegalStateException("table namespace cannot contain ':' char");
    if (tableNamespace.contains(TABLE_PHYSICAL_NAME_DELIM))
      throw new IllegalStateException("table namespace cannot contain '"
          + TABLE_PHYSICAL_NAME_DELIM + "' char");
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

  protected String physicalName;

  public abstract String getPhysicalName();

  protected String namespaceQualifiedPhysicalName;

  public abstract String getNamespaceQualifiedPhysicalName();

  protected String qualifiedLogicalName;

  public abstract String getQualifiedLogicalName();

  protected String qualifiedPhysicalNamespace;

  public abstract String getQualifiedPhysicalNamespace();

  public static String qualifiedLogicalNameFor(Table table, StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    // note: for logical names no not prepend the root
    // path as it is necessarily the same for all
    // tables even in a multi-tenant / volume environment
    if (table.getTableVolumeName() != null) {
      if (table.getTableVolumeName().contains(TABLE_PHYSICAL_NAME_DELIM))
        throw new IllegalStateException("volume prefix cannot contain '"
            + TABLE_PHYSICAL_NAME_DELIM + "' char");
      name.append(table.getTableVolumeName());
      name.append(TABLE_LOGICAL_NAME_DELIM);
    } else if (context != null && context.hasTableVolumeName()) {
      if (context.getTableVolumeName().contains(TABLE_PHYSICAL_NAME_DELIM))
        throw new IllegalStateException("volume prefix cannot contain '"
            + TABLE_PHYSICAL_NAME_DELIM + "' char");
      name.append(context.getTableVolumeName());
      name.append(TABLE_LOGICAL_NAME_DELIM);
    }
    name.append(table.getNamespace());
    name.append(TABLE_LOGICAL_NAME_DELIM);
    name.append(table.getName());
    return name.toString();
  }

  public static String qualifiedLogicalNameFor(QName typeName, StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    // note: for logical names no not prepend the root
    // path as it is necessarily the same for all
    // tables even in a multi-tenant / volume environment
    if (context != null && context.hasTableVolumeName()) {
      if (context.getTableVolumeName().contains(TABLE_PHYSICAL_NAME_DELIM))
        throw new IllegalStateException("volume prefix cannot contain '"
            + TABLE_PHYSICAL_NAME_DELIM + "' char");
      name.append(context.getTableVolumeName());
      name.append(TABLE_LOGICAL_NAME_DELIM);
    }
    name.append(typeName.toString());
    return name.toString();
  }

  public static String physicalNameFor(String tableName, StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    name.append(tableName);
    return name.toString();
  }

  public static String namespaceQualifiedPhysicalNameFor(String namespace,
      String physicalTableName, StoreMappingContext context) {
    String delim = TABLE_PHYSICAL_NAME_DELIM;
    if (context.hasTablePhysicalNamespaceDelim()) {
      delim = context.getTablePhysicalNamespaceDelim();
    }
    StringBuilder name = new StringBuilder();
    // prepend the root path is exists
    String rootPath = StoreMapping.getInstance().tableNamespaceRoot();
    if (rootPath != null) {
      name.append(rootPath);
      name.append(delim);
    }
    if (context != null && context.hasTableVolumeName()) {
      name.append(context.getTableVolumeName());
      name.append(delim);
    }
    name.append(namespace);
    name.append(delim);
    name.append(physicalTableName);
    return name.toString();
  }

  public static String qualifiedPhysicalNamespaceFor(Table table, StoreMappingContext context) {
    StringBuilder name = new StringBuilder();

    String delim = TABLE_PHYSICAL_NAME_DELIM;
    if (context.hasTablePhysicalNamespaceDelim()) {
      delim = context.getTablePhysicalNamespaceDelim();
    }
    String physicalNamespace = table.getNamespace().replaceAll(TABLE_LOGICAL_NAME_DELIM, delim);
    
    String rootPath = StoreMapping.getInstance().tableNamespaceRoot();
    // prepend the root path is exists
    if (rootPath != null) {
      name.append(rootPath);
      name.append(delim);
    }
    // prepend the volume name from either the
    // table config or volume name or cont4xt volume name
    String volumeName = findVolumeName(table, context);
	if (volumeName != null && !physicalNamespace.startsWith(volumeName)) {
	    name.append(volumeName);
	    name.append(delim);	
	}
	
    name.append(table.getNamespace());
    return name.toString();
  }
  
  private static String findVolumeName(Table table, StoreMappingContext context)
  {
	    if (table.getTableVolumeName() != null) {
	        if (context != null && context.hasTableVolumeName()
	            && !table.getTableVolumeName().equals(context.getTableVolumeName())) {
	          log.warn("overriding table volumme '" + table.getTableVolumeName()
	              + "' with context volue '" + context.getTableVolumeName() + "'");
	          return context.getTableVolumeName();
 	        } else {
 	        	return table.getTableVolumeName();
	           
	        }
	      } else if (context != null && context.hasTableVolumeName()) {
	    	  return context.getTableVolumeName();
 	      }
	  return null;
  }

//  public static String qualifiedPhysicalNamespaceFor(String namespace, StoreMappingContext context) {
//    StringBuilder name = new StringBuilder();
//    String delim = TABLE_PHYSICAL_NAME_DELIM;
//    if (context.hasTablePhysicalNamespaceDelim()) {
//      delim = context.getTablePhysicalNamespaceDelim();
//    }
//    String rootPath = StoreMapping.getInstance().tableNamespaceRoot();
//    if (rootPath != null) {
//      name.append(rootPath);
//      name.append(delim);
//    }
//    if (context != null && context.hasTableVolumeName()) {
//      name.append(context.getTableVolumeName());
//      name.append(delim);
//    }
//
//    name.append(namespace);
//    return name.toString();
//  }

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

  private String tableVolumeNameVar = null;

  public String tableVolumeName() {
    if (tableVolumeNameVar == null) {
      tableVolumeNameVar = getTablePropertyString(
          ConfigurationProperty.CLOUDGRAPH___TABLE___VOLUME___NAME,
          this.table.getTableVolumeName(), null);
    }
    return this.tableVolumeNameVar;
  }

  private String tableNamespaceRootVar = null;

  public String tableNamespaceRoot() {
    if (tableNamespaceRootVar == null) {
      tableNamespaceRootVar = getTablePropertyString(
          ConfigurationProperty.CLOUDGRAPH___TABLE___NAMESPACE___ROOT,
          this.table.getTableNamespaceRoot(), null);
    }
    return this.tableNamespaceRootVar;
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