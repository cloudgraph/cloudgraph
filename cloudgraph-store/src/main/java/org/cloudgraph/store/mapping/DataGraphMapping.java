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

import javax.xml.namespace.QName;

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
  static final String GRAPH_PATH_DELIM = "/";
  private DataGraph graph;
  private TableMapping table;
  private Map<MetaFieldName, MetaKeyFieldMapping> metaRowKeyFieldMap = new HashMap<MetaFieldName, MetaKeyFieldMapping>();
  private List<MetaKeyFieldMapping> metaRowKeyFieldList = new ArrayList<MetaKeyFieldMapping>();
  private Map<MetaFieldName, ColumnKeyFieldMapping> metaColumnKeyFieldMap = new HashMap<MetaFieldName, ColumnKeyFieldMapping>();
  private List<DataRowKeyFieldMapping> dataRowKeyFieldList = new ArrayList<DataRowKeyFieldMapping>();
  private Map<String, DataRowKeyFieldMapping> pathToDataRowKeyMap = new HashMap<String, DataRowKeyFieldMapping>();
  private Map<commonj.sdo.Property, DataRowKeyFieldMapping> propertyToDataRowKeyMap = new HashMap<commonj.sdo.Property, DataRowKeyFieldMapping>();
  private List<ConstantRowKeyFieldMapping> constantRowKeyFieldList = new ArrayList<ConstantRowKeyFieldMapping>();
  private List<KeyFieldMapping> rowKeyFieldList = new ArrayList<KeyFieldMapping>();
  private List<KeyFieldMapping> columnKeyFieldList = new ArrayList<KeyFieldMapping>();
  private Map<String, Property> propertyNameToPropertyMap = new HashMap<String, Property>();

  private byte[] rowKeyFieldDelimiterBytes;
  private byte[] columnKeyFieldDelimiterBytes;
  private byte[] columnKeySequenceDelimiterBytes;
  private byte[] columnKeyReferenceMetadataDelimiterBytes;
  private static Charset charset = Charset.forName(CoreConstants.UTF8_ENCODING);

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
          + "' specified for table, '" + table.getQualifiedLogicalName() + "'", e);
    }

    // validate the type against the URI
    Type typeResult = PlasmaTypeHelper.INSTANCE.getType(graph.getUri(), graph.getType());
    if (typeResult == null)
      throw new StoreMappingException("invalid graph URI/type combination '" + graph.getUri() + "/"
          + graph.getType() + "' specified for table, '" + table.getQualifiedLogicalName()
          + "' - type does not exist");

    for (Property prop : graph.getProperties())
      propertyNameToPropertyMap.put(prop.getName(), prop);

    int totalRowKeyFields = this.graph.getRowKeyModel().getRowKeyFields().size();
    int seqNum = 1;
    for (RowKeyField rowKeyField : this.graph.getRowKeyModel().getRowKeyFields()) {
      if (rowKeyField.getMetaField() != null) {
        MetaField predefinedField = rowKeyField.getMetaField();
        MetaKeyFieldMapping predefinedFieldConfig = new MetaKeyFieldMapping(this, predefinedField,
            seqNum, totalRowKeyFields);
        metaRowKeyFieldMap.put(predefinedField.getName(), predefinedFieldConfig);
        metaRowKeyFieldList.add(predefinedFieldConfig);
        this.rowKeyFieldList.add(predefinedFieldConfig);
      } else if (rowKeyField.getDataField() != null) {
        DataField userField = rowKeyField.getDataField();
        DataRowKeyFieldMapping userFieldConfig = new DataRowKeyFieldMapping(this, userField,
            seqNum, totalRowKeyFields);
        dataRowKeyFieldList.add(userFieldConfig);
        if (this.pathToDataRowKeyMap.get(userFieldConfig.getPropertyPath()) != null)
          throw new StoreMappingException("a user defined token path '"
              + userFieldConfig.getPathExpression() + "' already exists with property path '"
              + userFieldConfig.getPropertyPath() + "' for data graph of type, "
              + this.graph.getUri() + "#" + this.graph.getType());
        this.pathToDataRowKeyMap.put(userFieldConfig.getPropertyPath(), userFieldConfig);
        this.propertyToDataRowKeyMap.put(userFieldConfig.getEndpointProperty(), userFieldConfig);
        this.rowKeyFieldList.add(userFieldConfig);
      } else if (rowKeyField.getConstantField() != null) {
        ConstantField constantField = rowKeyField.getConstantField();
        ConstantRowKeyFieldMapping constantFieldConfig = new ConstantRowKeyFieldMapping(this,
            constantField, seqNum, totalRowKeyFields);
        constantRowKeyFieldList.add(constantFieldConfig);
        this.rowKeyFieldList.add(constantFieldConfig);
      } else
        throw new StoreMappingException("unexpected row key model field instance, "
            + rowKeyField.getClass().getName());
      seqNum++;
    }

    ColumnKeyModel columnKeyModel = this.graph.getColumnKeyModel();
    if (columnKeyModel.getReferenceMetadataDelimiter() == null)
      throw new StoreMappingException("found invalid (null) column metadata delimiter "
          + "for table, " + this.table.getQualifiedLogicalName() + ", for graph "
          + this.graph.getUri() + "#" + this.graph.getType());
    if (!columnKeyModel.isFieldsFixedLength()) {
      if (columnKeyModel.getFieldDelimiter() == null)
        throw new StoreMappingException("found invalid (null) column field delimiter "
            + "for fixed length column model " + "for table, "
            + this.table.getQualifiedLogicalName() + ", for graph " + this.graph.getUri() + "#"
            + this.graph.getType());
    } else {
      if (columnKeyModel.getFieldLength() == null)
        throw new StoreMappingException(
            "found invalid (null) column field lengh for fixed length column model "
                + "for table, " + this.table.getQualifiedLogicalName() + ", for graph "
                + this.graph.getUri() + "#" + this.graph.getType());
    }
    if (columnKeyModel.getSequenceDelimiter() == null)
      throw new StoreMappingException("found invalid (null) column sequence delimiter "
          + "for table, " + this.table.getQualifiedLogicalName() + ", for graph "
          + this.graph.getUri() + "#" + this.graph.getType());
    if (columnKeyModel.getReferenceMetadataDelimiter().equals(columnKeyModel.getFieldDelimiter()))
      throw new StoreMappingException("found duplicate (" + columnKeyModel.getFieldDelimiter()
          + ") column metadata delimiter " + "for table, " + this.table.getQualifiedLogicalName()
          + ", for graph " + this.graph.getUri() + "#" + this.graph.getType());
    if (columnKeyModel.getSequenceDelimiter().equals(columnKeyModel.getFieldDelimiter()))
      throw new StoreMappingException("found duplicate (" + columnKeyModel.getFieldDelimiter()
          + ") column sequence delimiter " + "for table, " + this.table.getQualifiedLogicalName()
          + ", for graph " + this.graph.getUri() + "#" + this.graph.getType());
    if (columnKeyModel.getReferenceMetadataDelimiter()
        .equals(columnKeyModel.getSequenceDelimiter()))
      throw new StoreMappingException("found duplicate (" + columnKeyModel.getSequenceDelimiter()
          + ") column metadata delimiter " + "for table, " + this.table.getQualifiedLogicalName()
          + ", for graph " + this.graph.getUri() + "#" + this.graph.getType());

    int totalColumnKeyFields = columnKeyModel.getColumnKeyFields().size();
    seqNum = 1;
    for (ColumnKeyField ctoken : columnKeyModel.getColumnKeyFields()) {
      ColumnKeyFieldMapping columnFieldConfig = new ColumnKeyFieldMapping(this, ctoken, seqNum,
          totalColumnKeyFields);
      metaColumnKeyFieldMap.put(ctoken.getName(), columnFieldConfig);
      this.columnKeyFieldList.add(columnFieldConfig);
      seqNum++;
    }
  }

  private QName qualifiedName;

  public QName getQualifiedName() {
    if (this.qualifiedName == null) {
      this.qualifiedName = new QName(this.getGraph().getUri(), this.getGraph().getType());
    }
    return this.qualifiedName;
  }

  private String qualifiedLogicalName;

  public String getQualifiedLogicalName() {
    if (this.qualifiedLogicalName == null) {
      QName typeName = getQualifiedName();
      if (DynamicTableMapping.class.isAssignableFrom(this.getTable().getClass()))
        this.qualifiedLogicalName = qualifiedLogicalNameFor(this.getTable().getTable(), typeName,
            DynamicTableMapping.class.cast(this.getTable()).getMappingContext());
      else
        this.qualifiedLogicalName = qualifiedLogicalNameFor(this.getTable().getTable(), typeName,
            null);

    }
    return this.qualifiedLogicalName;
  }

  public static String qualifiedLogicalNameFor(Table table, QName typeName,
      StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    String typeNameString = typeName.toString();
    if (table.getTableVolumeName() != null) {
      name.append(table.getTableVolumeName());
      name.append(GRAPH_PATH_DELIM);
    } else if (context != null && context.hasTableVolumeName()
        && !typeNameString.contains(context.getTableVolumeName())) {
      name.append(context.getTableVolumeName());
      name.append(GRAPH_PATH_DELIM);
    }
    name.append(typeName.toString());
    return name.toString();
  }

  public static String qualifiedLogicalNameFor(QName typeName, StoreMappingContext context) {
    StringBuilder name = new StringBuilder();
    String typeNameString = typeName.toString();
    if (context != null && context.hasTableVolumeName()
        && !typeNameString.contains(context.getTableVolumeName())) {
      name.append(context.getTableVolumeName());
      name.append(GRAPH_PATH_DELIM);
    }
    name.append(typeName.toString());
    return name.toString();
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

  public List<MetaKeyFieldMapping> getPreDefinedRowKeyFields() {
    return this.metaRowKeyFieldList;
  }

  public MetaKeyFieldMapping getPreDefinedRowKeyField(MetaFieldName name) {
    return this.metaRowKeyFieldMap.get(name);
  }

  public String getRowKeyFieldDelimiter() {
    return this.graph.getRowKeyModel().getFieldDelimiter();
  }

  public byte[] getRowKeyFieldDelimiterBytes() {
    if (rowKeyFieldDelimiterBytes == null) {
      this.rowKeyFieldDelimiterBytes = this.graph.getRowKeyModel().getFieldDelimiter()
          .getBytes(charset);
    }
    return rowKeyFieldDelimiterBytes;
  }

  public boolean hasUserDefinedRowKeyFields() {
    return this.dataRowKeyFieldList.size() > 0;
  }

  public List<DataRowKeyFieldMapping> getUserDefinedRowKeyFields() {
    return dataRowKeyFieldList;
  }

  public DataRowKeyFieldMapping getUserDefinedRowKeyField(String path) {
    return this.pathToDataRowKeyMap.get(path);
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
  public DataRowKeyFieldMapping findUserDefinedRowKeyField(commonj.sdo.Property property) {
    return this.propertyToDataRowKeyMap.get(property);
  }

  public ColumnKeyFieldMapping getColumnKeyField(MetaFieldName name) {
    return metaColumnKeyFieldMap.get(name);
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
          .getBytes(charset);
    }
    return columnKeyFieldDelimiterBytes;
  }

  public byte[] getColumnKeySequenceDelimiterBytes() {
    if (columnKeySequenceDelimiterBytes == null) {
      this.columnKeySequenceDelimiterBytes = this.graph.getColumnKeyModel().getSequenceDelimiter()
          .getBytes(charset);
    }
    return columnKeySequenceDelimiterBytes;
  }

  public byte[] getColumnKeyReferenceMetadataDelimiterBytes() {
    if (columnKeyReferenceMetadataDelimiterBytes == null) {
      this.columnKeyReferenceMetadataDelimiterBytes = this.graph.getColumnKeyModel()
          .getReferenceMetadataDelimiter().getBytes(charset);
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
