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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudgraph.store.mapping.ColumnKeyField;
import org.cloudgraph.store.mapping.ColumnKeyModel;
import org.cloudgraph.store.mapping.DataGraph;
import org.cloudgraph.store.mapping.PreDefinedFieldName;
import org.cloudgraph.store.mapping.PredefinedField;
import org.cloudgraph.store.mapping.Property;
import org.cloudgraph.store.mapping.RowKeyField;
import org.cloudgraph.store.mapping.UserDefinedField;
import org.plasma.runtime.ConfigurationException;
import org.plasma.runtime.PlasmaRuntime;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.Type;

/**
 * Encapsulates logic related to access of graph specific configuration
 * information.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class DataGraphMapping {
  private DataGraph graph;
  private TableMapping table;
  private Map<PreDefinedFieldName, PreDefinedKeyFieldMapping> preDefinedRowKeyFieldMap = new HashMap<PreDefinedFieldName, PreDefinedKeyFieldMapping>();
  private List<PreDefinedKeyFieldMapping> preDefinedRowKeyFieldList = new ArrayList<PreDefinedKeyFieldMapping>();
  private Map<PreDefinedFieldName, ColumnKeyFieldMapping> preDefinedColumnKeyFieldMap = new HashMap<PreDefinedFieldName, ColumnKeyFieldMapping>();
  private List<UserDefinedRowKeyFieldMapping> userDefinedRowKeyFieldList = new ArrayList<UserDefinedRowKeyFieldMapping>();
  private Map<String, UserDefinedRowKeyFieldMapping> pathToUserDefinedRowKeyMap = new HashMap<String, UserDefinedRowKeyFieldMapping>();
  private Map<commonj.sdo.Property, UserDefinedRowKeyFieldMapping> propertyToUserDefinedRowKeyMap = new HashMap<commonj.sdo.Property, UserDefinedRowKeyFieldMapping>();
  private List<KeyFieldMapping> rowKeyFieldList = new ArrayList<KeyFieldMapping>();
  private List<KeyFieldMapping> columnKeyFieldList = new ArrayList<KeyFieldMapping>();
  private Map<String, Property> propertyNameToPropertyMap = new HashMap<String, Property>();

  private byte[] rowKeyFieldDelimiterBytes;
  private byte[] columnKeyFieldDelimiterBytes;
  private byte[] columnKeySequenceDelimiterBytes;
  private byte[] columnKeyReferenceMetadataDelimiterBytes;

  @SuppressWarnings("unused")
  private DataGraphMapping() {
  }

  public DataGraphMapping(DataGraph graph, TableMapping table) {
    super();
    this.graph = graph;
    this.table = table;

    // validate the URI
    try {
      PlasmaRuntime.getInstance().getSDONamespaceByURI(graph.getUri());
    } catch (ConfigurationException e) {
      throw new StoreMappingException("invalid graph URI '" + graph.getUri()
          + "' specified for table, '" + table.getName() + "'", e);
    }

    // validate the type against the URI
    Type typeResult = PlasmaTypeHelper.INSTANCE.getType(graph.getUri(), graph.getType());
    if (typeResult == null)
      throw new StoreMappingException("invalid graph URI/type combination '" + graph.getUri() + "/"
          + graph.getType() + "' specified for table, '" + table.getName()
          + "' - type does not exist");

    for (Property prop : graph.getProperties())
      propertyNameToPropertyMap.put(prop.getName(), prop);

    int totalRowKeyFields = this.graph.getRowKeyModel().getRowKeyFields().size();
    int seqNum = 1;
    for (RowKeyField rowKeyField : this.graph.getRowKeyModel().getRowKeyFields()) {
      if (rowKeyField.getPredefinedField() != null) {
        PredefinedField predefinedField = rowKeyField.getPredefinedField();
        PreDefinedKeyFieldMapping predefinedFieldConfig = new PreDefinedKeyFieldMapping(
            predefinedField, seqNum, totalRowKeyFields);
        preDefinedRowKeyFieldMap.put(predefinedField.getName(), predefinedFieldConfig);
        preDefinedRowKeyFieldList.add(predefinedFieldConfig);
        this.rowKeyFieldList.add(predefinedFieldConfig);
      } else if (rowKeyField.getUserDefinedField() != null) {
        UserDefinedField userField = rowKeyField.getUserDefinedField();
        UserDefinedRowKeyFieldMapping userFieldConfig = new UserDefinedRowKeyFieldMapping(this,
            userField, seqNum, totalRowKeyFields);
        userDefinedRowKeyFieldList.add(userFieldConfig);
        if (this.pathToUserDefinedRowKeyMap.get(userFieldConfig.getPropertyPath()) != null)
          throw new StoreMappingException("a user defined token path '"
              + userFieldConfig.getPathExpression() + "' already exists with property path '"
              + userFieldConfig.getPropertyPath() + "' for data graph of type, "
              + this.graph.getUri() + "#" + this.graph.getType());
        this.pathToUserDefinedRowKeyMap.put(userFieldConfig.getPropertyPath(), userFieldConfig);
        this.propertyToUserDefinedRowKeyMap.put(userFieldConfig.getEndpointProperty(),
            userFieldConfig);
        this.rowKeyFieldList.add(userFieldConfig);
      } else
        throw new StoreMappingException("unexpected row key model field instance, "
            + rowKeyField.getClass().getName());
      seqNum++;
    }

    ColumnKeyModel columnKeyModel = this.graph.getColumnKeyModel();
    if (columnKeyModel.getReferenceMetadataDelimiter() == null)
      throw new StoreMappingException("found invalid (null) column metadata delimiter "
          + "for table, " + this.table.getName() + ", for graph " + this.graph.getUri() + "#"
          + this.graph.getType());
    if (columnKeyModel.getFieldDelimiter() == null)
      throw new StoreMappingException("found invalid (null) column field delimiter "
          + "for table, " + this.table.getName() + ", for graph " + this.graph.getUri() + "#"
          + this.graph.getType());
    if (columnKeyModel.getSequenceDelimiter() == null)
      throw new StoreMappingException("found invalid (null) column sequence delimiter "
          + "for table, " + this.table.getName() + ", for graph " + this.graph.getUri() + "#"
          + this.graph.getType());
    if (columnKeyModel.getReferenceMetadataDelimiter().equals(columnKeyModel.getFieldDelimiter()))
      throw new StoreMappingException("found duplicate (" + columnKeyModel.getFieldDelimiter()
          + ") column metadata delimiter " + "for table, " + this.table.getName() + ", for graph "
          + this.graph.getUri() + "#" + this.graph.getType());
    if (columnKeyModel.getSequenceDelimiter().equals(columnKeyModel.getFieldDelimiter()))
      throw new StoreMappingException("found duplicate (" + columnKeyModel.getFieldDelimiter()
          + ") column sequence delimiter " + "for table, " + this.table.getName() + ", for graph "
          + this.graph.getUri() + "#" + this.graph.getType());
    if (columnKeyModel.getReferenceMetadataDelimiter()
        .equals(columnKeyModel.getSequenceDelimiter()))
      throw new StoreMappingException("found duplicate (" + columnKeyModel.getSequenceDelimiter()
          + ") column metadata delimiter " + "for table, " + this.table.getName() + ", for graph "
          + this.graph.getUri() + "#" + this.graph.getType());

    int totalColumnKeyFields = columnKeyModel.getColumnKeyFields().size();
    seqNum = 1;
    for (ColumnKeyField ctoken : columnKeyModel.getColumnKeyFields()) {
      ColumnKeyFieldMapping columnFieldConfig = new ColumnKeyFieldMapping(ctoken, seqNum,
          totalColumnKeyFields);
      preDefinedColumnKeyFieldMap.put(ctoken.getName(), columnFieldConfig);
      this.columnKeyFieldList.add(columnFieldConfig);
      seqNum++;
    }
  }

  public DataGraph getGraph() {
    return this.graph;
  }

  public ColumnKeyModel getColumnKeyModel() {
    return this.graph.getColumnKeyModel();
  }

  public Type getRootType() {
    return PlasmaTypeHelper.INSTANCE.getType(this.graph.getUri(), this.graph.getType());
  }

  public List<Property> getProperties() {
    return this.graph.properties;
  }

  public Property findProperty(String name) {
    return this.propertyNameToPropertyMap.get(name);
  }

  public List<PreDefinedKeyFieldMapping> getPreDefinedRowKeyFields() {
    return this.preDefinedRowKeyFieldList;
  }

  public PreDefinedKeyFieldMapping getPreDefinedRowKeyField(PreDefinedFieldName name) {
    return this.preDefinedRowKeyFieldMap.get(name);
  }

  public String getRowKeyFieldDelimiter() {
    return this.graph.getRowKeyModel().getFieldDelimiter();
  }

  public byte[] getRowKeyFieldDelimiterBytes() {
    if (rowKeyFieldDelimiterBytes == null) {
      this.rowKeyFieldDelimiterBytes = this.graph.getRowKeyModel().getFieldDelimiter()
          .getBytes(Charset.forName(CoreConstants.UTF8_ENCODING));
    }
    return rowKeyFieldDelimiterBytes;
  }

  public boolean hasUserDefinedRowKeyFields() {
    return this.userDefinedRowKeyFieldList.size() > 0;
  }

  public List<UserDefinedRowKeyFieldMapping> getUserDefinedRowKeyFields() {
    return userDefinedRowKeyFieldList;
  }

  public UserDefinedRowKeyFieldMapping getUserDefinedRowKeyField(String path) {
    return this.pathToUserDefinedRowKeyMap.get(path);
  }

  public List<KeyFieldMapping> getRowKeyFields() {
    return this.rowKeyFieldList;
  }

  public List<KeyFieldMapping> getColumnKeyFields() {
    return this.columnKeyFieldList;
  }

  /**
   * Returns the row key field config for the given path endpoint property, or
   * null if not exists. An endpoint property is a property which terminates an
   * SDO XPath.
   * 
   * @param property
   *          the endpoint property
   * @return the row key field config for the given path endpoint property, or
   *         null if not exists.
   */
  public UserDefinedRowKeyFieldMapping findUserDefinedRowKeyField(commonj.sdo.Property property) {
    return this.propertyToUserDefinedRowKeyMap.get(property);
  }

  public ColumnKeyFieldMapping getColumnKeyField(PreDefinedFieldName name) {
    return preDefinedColumnKeyFieldMap.get(name);
  }

  public String getColumnKeyFieldDelimiter() {
    return this.graph.getColumnKeyModel().getFieldDelimiter();
  }

  public String getColumnKeySequenceDelimiter() {
    return this.graph.getColumnKeyModel().getSequenceDelimiter();
  }

  public byte[] getColumnKeyFieldDelimiterBytes() {
    if (columnKeyFieldDelimiterBytes == null) {
      this.columnKeyFieldDelimiterBytes = this.graph.getColumnKeyModel().getFieldDelimiter()
          .getBytes(Charset.forName(CoreConstants.UTF8_ENCODING));
    }
    return columnKeyFieldDelimiterBytes;
  }

  public byte[] getColumnKeySequenceDelimiterBytes() {
    if (columnKeySequenceDelimiterBytes == null) {
      this.columnKeySequenceDelimiterBytes = this.graph.getColumnKeyModel().getSequenceDelimiter()
          .getBytes(Charset.forName(CoreConstants.UTF8_ENCODING));
    }
    return columnKeySequenceDelimiterBytes;
  }

  public byte[] getColumnKeyReferenceMetadataDelimiterBytes() {
    if (columnKeyReferenceMetadataDelimiterBytes == null) {
      this.columnKeyReferenceMetadataDelimiterBytes = this.graph.getColumnKeyModel()
          .getReferenceMetadataDelimiter().getBytes(Charset.forName(CoreConstants.UTF8_ENCODING));
    }
    return columnKeyReferenceMetadataDelimiterBytes;
  }

  /**
   * Returns the configured table for this data graph config.
   * 
   * @return the configured table for this data graph config.
   */
  public TableMapping getTable() {
    return table;
  }

}
