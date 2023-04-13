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

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;

import javax.xml.namespace.QName;

import commonj.sdo.Type;

public interface MappingConfiguration {

  public void marshal(OutputStream stream);

  public List<Property> getProperties();

  public Property findProperty(String name);

  public ConfigProperties getConfigProperties();

  /**
   * Returns all tables not dynamically defined, that is those tables defined
   * within a static mapping resource / file.
   * 
   * @param context
   *          the mapping context
   * @return all tables not dynamically defined, that is those tables defined
   *         within a static mapping resource / file.
   */
  public List<TableMapping> getStaticTables(StoreMappingContext context);

  /**
   * Returns all tables dynamically defined, that is those tables defined
   * dynamically at runtime
   * 
   * @param context
   *          the mapping context
   * @return all tables dynamically defined, that is those tables defined
   *         dynamically at runtime
   */
  public List<TableMapping> getDynamicTables(StoreMappingContext context);

  /**
   * Returns a table configuration for the given qualified SDO Type name or null
   * if not found.
   * 
   * @param typeName
   *          the qualified name of an SDO Type
   * @return the table configuration or null if not found
   */
  public TableMapping findTable(QName typeName, StoreMappingContext context);

  /**
   * Returns a table configuration for the given qualified SDO Type name.
   * 
   * @param typeName
   *          the qualified name of an SDO Type
   * @return the table configuration
   * @throws StoreMappingException
   *           if the given name is not found
   */
  public TableMapping getTable(QName typeName, StoreMappingContext context);

  /**
   * Returns a table configuration for the given SDO Type or null if not found.
   * 
   * @param type
   *          the SDO Type
   * @return the table configuration or null if not found
   */
  public TableMapping findTable(Type type, StoreMappingContext context);

  /**
   * Returns a table configuration for the given SDO Type.
   * 
   * @param type
   *          the SDO Type
   * @return the table configuration
   * @throws StoreMappingException
   *           if the given type is not found
   */
  public TableMapping getTable(Type type, StoreMappingContext context);

  /**
   * Returns a table configuration based on the given table name.
   * 
   * @param tableName
   *          the table name or null if not found.
   * @return the table configuration or null if not found.
   */
  public TableMapping findTableByQualifiedLogicalName(String tableName, StoreMappingContext context);

  /**
   * Returns a table mapping based on the given table name.
   * 
   * @param tableName
   *          the table name
   * @return the table configuration
   * @throws StoreMappingException
   *           if the given name is not found
   */
  public TableMapping getTableByQualifiedLogicalName(String qualifiedTableName,
      StoreMappingContext context);

  // /**
  // * Returns a table name for the given qualified SDO Type name.
  // *
  // * @param typeName
  // * the qualified name of an SDO Type
  // * @return the table name
  // */
  // public String getNamespaceQualifiedPhysicalTableName(QName typeName,
  // StoreMappingContext context);

  /**
   * Adds the given configuration
   * 
   * @param table
   *          the table configuration
   */
  public void addTable(TableMapping table);

  /**
   * Adds the given configuration if not already exists
   * 
   * @param table
   *          the table configuration
   */
  public void addTableIfNotExists(TableMapping table);

  /**
   * Adds the given configuration
   * 
   * @param table
   *          the table configuration
   */
  public void removeTable(TableMapping table);

  /**
   * Adds the given configuration if exists
   * 
   * @param table
   *          the table configuration
   */
  public void removeTableIfExists(TableMapping table);

  /**
   * Returns a data graph config for the given qualified SDO Type name or null
   * of not exists.
   * 
   * @param typeName
   *          the qualified name of an SDO Type
   * @return a data graph mapping for the given qualified SDO Type name or null
   *         of not exists.
   */
  public DataGraphMapping findDataGraph(QName qname, StoreMappingContext context);

  /**
   * Returns a data graph mapping for the given qualified SDO Type name.
   * 
   * @param typeName
   *          the qualified name of an SDO Type
   * @return a data graph mapping for the given qualified SDO Type name.
   * @throws StoreMappingException
   *           if no configured data graph exists for the given qualified SDO
   *           Type name
   */
  public DataGraphMapping getDataGraph(QName qname, StoreMappingContext context);

  public Charset getCharset();

  public boolean uniqueChecks();

  public boolean tombstoneRows();

  public boolean tombstoneRowsOverwriteable();

  public String tableNamespaceRoot();

  public String tableVolumeName();

  public boolean optimisticConcurrency();

  // public String qualifiedLogicalTableNameFromPhysicalTablePath(String
  // namespace, String tableName,
  // StoreMappingContext context);

}