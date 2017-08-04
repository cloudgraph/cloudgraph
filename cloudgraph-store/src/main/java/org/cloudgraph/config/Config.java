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
package org.cloudgraph.config;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;

import javax.xml.namespace.QName;

import commonj.sdo.Type;

public interface Config {

  public void marshal(OutputStream stream);

  public List<Property> getProperties();

  public Property findProperty(String name);

  public ConfigProperties getConfigProperties();

  /**
   * Returns a table configuration for the given qualified SDO Type name or null
   * if not found.
   * 
   * @param typeName
   *          the qualified name of an SDO Type
   * @return the table configuration or null if not found
   */
  public TableConfig findTable(QName typeName);

  /**
   * Returns a table configuration for the given qualified SDO Type name.
   * 
   * @param typeName
   *          the qualified name of an SDO Type
   * @return the table configuration
   * @throws CloudGraphConfigurationException
   *           if the given name is not found
   */
  public TableConfig getTable(QName typeName);

  /**
   * Returns a table configuration for the given SDO Type or null if not found.
   * 
   * @param type
   *          the SDO Type
   * @return the table configuration or null if not found
   */
  public TableConfig findTable(Type type);

  /**
   * Returns a table configuration for the given SDO Type.
   * 
   * @param type
   *          the SDO Type
   * @return the table configuration
   * @throws CloudGraphConfigurationException
   *           if the given type is not found
   */
  public TableConfig getTable(Type type);

  /**
   * Returns a table configuration based on the given table name.
   * 
   * @param tableName
   *          the table name or null if not found.
   * @return the table configuration or null if not found.
   */
  public TableConfig findTable(String tableName);

  /**
   * Returns a table configuration based on the given table name.
   * 
   * @param tableNamespace
   *          the table namespace
   * @param tableName
   *          the table name
   * @return the table configuration
   * @throws CloudGraphConfigurationException
   *           if the given name is not found
   */
  public TableConfig getTable(String tableNamespace, String tableName);

  /**
   * Returns a table name for the given qualified SDO Type name.
   * 
   * @param typeName
   *          the qualified name of an SDO Type
   * @return the table name
   */
  public String getTableName(QName typeName);

  /**
   * Adds the given configuration
   * 
   * @param table
   *          the table configuration
   */
  public void addTable(TableConfig table);

  /**
   * Adds the given configuration
   * 
   * @param table
   *          the table configuration
   */
  public void removeTable(TableConfig table);

  /**
   * Returns a data graph config for the given qualified SDO Type name or null
   * of not exists.
   * 
   * @param typeName
   *          the qualified name of an SDO Type
   * @return a data graph config for the given qualified SDO Type name or null
   *         of not exists.
   */
  public DataGraphConfig findDataGraph(QName qname);

  /**
   * Returns a data graph config for the given qualified SDO Type name.
   * 
   * @param typeName
   *          the qualified name of an SDO Type
   * @return a data graph config for the given qualified SDO Type name.
   * @throws CloudGraphConfigurationException
   *           if no configured data graph exists for the given qualified SDO
   *           Type name
   */
  public DataGraphConfig getDataGraph(QName qname);

  public Charset getCharset();

  public boolean uniqueChecks();

  public boolean tombstoneRows();

  public boolean tombstoneRowsOverwriteable();

  public String maprdbTablePathPrefix();

}