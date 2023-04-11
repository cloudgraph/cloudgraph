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

import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.UnmarshalException;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.xml.namespace.QName;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atteo.classindex.ClassIndex;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.common.env.EnvProperties;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.helper.PlasmaTypeHelper;
import org.xml.sax.SAXException;

import commonj.sdo.Type;

/**
 * Configuration mapping marshaling and un-marshaling with data access
 * convenience methods. Looks for the Java command line
 * '-Dcloudgraph.configuration' setting for the name of the mapping
 * configuration file. If not found looks for the default file name
 * 'cloudgraph-config.xml'. The CloudGraph mapping configuration file must be
 * somewhere on the Java class path.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class StoreMapping implements MappingConfiguration {

  private static Log log = LogFactory.getLog(StoreMapping.class);
  private static volatile StoreMapping instance = null;
  private static final String PROPERTY_NAME_CLOUDGRAPH_CONFIG = "cloudgraph.configuration";
  private static final String DEFAULT_CONFIG_FILE_NAME = "cloudgraph-config.xml";
  private static final String PROPERTY_NAME_CLOUDGRAPH_PROPERTIES = "cloudgraph.properties";
  private static final String DEFAULT_PROPERTIES_FILE_NAME = "cloudgraph.properties";

  private CloudGraphStoreMapping config;
  private ConfigProperties configProperties;
  private Charset charset = Charset.forName(CoreConstants.UTF8_ENCODING);

  // private Map<QName, DataGraphMapping> graphURIToGraphMap = new
  // HashMap<QName, DataGraphMapping>();
  private Map<String, DataGraphMapping> graphURIToGraphMap = new HashMap<String, DataGraphMapping>();
  private Map<String, TableMapping> graphURIToTableMap = new HashMap<String, TableMapping>();
  // private Map<QName, TableMapping> graphURIToTableMap = new HashMap<QName,
  // TableMapping>();
  private Map<String, TableMapping> tableNameToTableMap = new HashMap<String, TableMapping>();
  private Map<String, Property> propertyNameToPropertyMap = new HashMap<String, Property>();

  private List<Class<?>> annotatedClasses = new ArrayList<Class<?>>();
  private ReadWriteLock lock = new ReentrantReadWriteLock();

  private TableMappingFactory tableMappingFactory = new DefaultTableMappingFactory();

  private StoreMapping() {
    log.debug("initializing...");
    try {
      configProperties = new ConfigProperties();
      configProperties.addConfiguration(new SystemConfiguration());

      String propertiesFileName = EnvProperties.instance().getProperty(
          PROPERTY_NAME_CLOUDGRAPH_PROPERTIES);
      if (propertiesFileName == null)
        propertiesFileName = DEFAULT_PROPERTIES_FILE_NAME;

      InputStream propertiesStream = CloudGraphStoreMapping.class
          .getResourceAsStream(propertiesFileName);
      if (propertiesStream == null)
        propertiesStream = StoreMapping.class.getClassLoader().getResourceAsStream(
            propertiesFileName);
      if (propertiesStream != null) {
        log.info("loading properties from bundled resource: " + propertiesFileName);
        configProperties.addConfiguration(new PropertiesConfiguration(propertiesFileName));
      } else {
        File propsConfigFile = new File(propertiesFileName);
        if (propsConfigFile.exists()) {
          log.info("loading properties from distributed cache or local file: " + propertiesFileName);
          configProperties.addConfiguration(new PropertiesConfiguration(propsConfigFile));
        } else {
          log.warn("could not properties from bundled resource, distributed cache or local file: "
              + propertiesFileName);
        }
      }

    } catch (ConfigurationException e) {
      throw new StoreMappingException(e);
    }

    try {

      for (Class<?> c : ClassIndex
          .getAnnotated(org.cloudgraph.store.mapping.annotation.Table.class))
        annotatedClasses.add(c);

      String mappingFileName = EnvProperties.instance()
          .getProperty(PROPERTY_NAME_CLOUDGRAPH_CONFIG);

      if (mappingFileName == null)
        mappingFileName = DEFAULT_CONFIG_FILE_NAME;

      InputStream stream = CloudGraphStoreMapping.class.getResourceAsStream(mappingFileName);
      if (stream == null)
        stream = StoreMapping.class.getClassLoader().getResourceAsStream(mappingFileName);
      if (stream != null) {
        if (annotatedClasses.size() > 0) {
          log.warn("found mapping file '" + mappingFileName + "' - ignoring "
              + annotatedClasses.size() + " annotated classes ");
        }
        StoreMappingDataBinding configBinding = new StoreMappingDataBinding(
            new StoreMappingValidationEventHandler());
        this.config = unmarshalConfig(stream, configBinding);
      } else {
        if (annotatedClasses.size() > 0) {
          try {
            this.config = deriveMapping();
          } catch (NoSuchFieldException | SecurityException e) {
            throw new StoreMappingException(e);
          }
        } else {
          throw new StoreMappingException(
              "could not find configuration file resource '"
                  + mappingFileName
                  + "' on the current classpath and could not derive configuration from annotated classes and packages"
                  + " - please ensure all annotated classes are on the classpath");

        }
      }

      for (Property prop : config.getProperties())
        propertyNameToPropertyMap.put(prop.getName(), prop);

      for (Table table : config.tables) {
        TableMapping tableConfig = this.tableMappingFactory.createStaticTableMapping(table, this);
        mapTable(tableConfig);
      }
    } catch (SAXException e) {
      throw new StoreMappingException(e);
    } catch (JAXBException e) {
      throw new StoreMappingException(e);
    }

  }

  private CloudGraphStoreMapping deriveMapping() throws NoSuchFieldException, SecurityException {
    if (log.isDebugEnabled())
      log.debug("deriving mapping");
    CloudGraphStoreMapping result = new CloudGraphStoreMapping();
    for (Class<?> c : this.annotatedClasses) {
      org.cloudgraph.store.mapping.annotation.Table tableAnnot = c
          .getAnnotation(org.cloudgraph.store.mapping.annotation.Table.class);
      if (log.isDebugEnabled())
        log.debug("discovered " + tableAnnot.name() + " table mapping");
      Table table = new Table();
      result.getTables().add(table);
      table.setName(tableAnnot.name());
      table.setDataColumnFamilyName(tableAnnot.dataColumnFamilyName());
      table.setTombstoneRows(tableAnnot.tombstoneRows());
      table.setTombstoneRowsOverwriteable(tableAnnot.tombstoneRowsOverwriteable());
      table.setUniqueChecks(tableAnnot.uniqueChecks());
      if (tableAnnot.hashAlgorithm().ordinal() != HashAlgorithmName.NONE.ordinal()) {
        HashAlgorithm hash = new HashAlgorithm();
        hash.setName(tableAnnot.hashAlgorithm());
        table.setHashAlgorithm(hash);
      }
      // add properties

      DataGraph dataGraph = new DataGraph();
      table.getDataGraphs().add(dataGraph);
      org.plasma.sdo.annotation.Type typeAnnot = c
          .getAnnotation(org.plasma.sdo.annotation.Type.class);

      String typeName = typeAnnot.name();
      if (typeName == null || typeName.trim().length() == 0)
        typeName = c.getSimpleName(); // use the enumeration class name
      dataGraph.setType(typeName);
      org.plasma.sdo.annotation.Namespace namespaceAnnot = c.getPackage().getAnnotation(
          org.plasma.sdo.annotation.Namespace.class);
      dataGraph.setUri(namespaceAnnot.uri());
      if (log.isDebugEnabled())
        log.debug("added data graph for type: " + dataGraph.getUri() + "#" + dataGraph.getType());

      ColumnKeyModel columnModel = new ColumnKeyModel();
      columnModel.setFieldDelimiter("|");
      columnModel.setReferenceMetadataDelimiter("#");
      columnModel.setSequenceDelimiter("@");
      dataGraph.setColumnKeyModel(columnModel);
      ColumnKeyField pkgColKeyField = new ColumnKeyField();
      pkgColKeyField.setName(MetaFieldName.PKG);
      columnModel.getColumnKeyFields().add(pkgColKeyField);
      ColumnKeyField typeColKeyField = new ColumnKeyField();
      typeColKeyField.setName(MetaFieldName.TYPE);
      columnModel.getColumnKeyFields().add(typeColKeyField);
      ColumnKeyField propColKeyField = new ColumnKeyField();
      propColKeyField.setName(MetaFieldName.PROPERTY);
      columnModel.getColumnKeyFields().add(propColKeyField);

      RowKeyModel rowKeyModel = new RowKeyModel();
      dataGraph.setRowKeyModel(rowKeyModel);
      rowKeyModel.setFieldDelimiter(tableAnnot.rowKeyFieldDelimiter());

      for (Object o : c.getEnumConstants()) {
        Enum<?> enm = (Enum<?>) o;
        Field field = c.getField(enm.name());
        org.cloudgraph.store.mapping.annotation.RowKeyField rowKeyFieldAnnot = field
            .getAnnotation(org.cloudgraph.store.mapping.annotation.RowKeyField.class);
        if (rowKeyFieldAnnot != null) {
          RowKeyField rowKeyField = new RowKeyField();
          rowKeyModel.getRowKeyFields().add(rowKeyField);
          DataField userDefinedField = new DataField();
          rowKeyField.setDataField(userDefinedField);
          userDefinedField.setPath(field.getName());

          userDefinedField.setCodecType(rowKeyFieldAnnot.codecType());
        }
      }

    }

    return result;
  }

  private void mapTable(TableMapping tableMapping) {
    if (this.tableNameToTableMap.get(tableMapping.getQualifiedLogicalName()) != null)
      throw new StoreMappingException("a table definition already exists for qualified name '"
          + tableMapping.getQualifiedLogicalName() + "'");
    this.tableNameToTableMap.put(tableMapping.getQualifiedLogicalName(), tableMapping);
    for (DataGraph graph : tableMapping.getTable().getDataGraphs()) {
      DataGraphMapping dataGraphConfig = new DataGraphMapping(graph, tableMapping);
      mapDataGraph(dataGraphConfig, tableMapping);
    }
  }

  private void unmapTable(TableMapping tableMapping) {
    if (this.tableNameToTableMap.get(tableMapping.getQualifiedLogicalName()) == null)
      throw new StoreMappingException("table definition does not exist exists for qualified name '"
          + tableMapping.getQualifiedLogicalName() + "'");

    for (DataGraph graph : tableMapping.getTable().getDataGraphs()) {
      DataGraphMapping dataGraphConfig = new DataGraphMapping(graph, tableMapping);
      unmapDataGraph(dataGraphConfig);
    }

    this.tableNameToTableMap.remove(tableMapping.getQualifiedLogicalName());
  }

  private void mapDataGraph(DataGraphMapping dataGraphMapping, TableMapping tableMapping) {

    QName qname = dataGraphMapping.getQualifiedName();

    PlasmaType configuredType = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(
        qname.getNamespaceURI(), qname.getLocalPart());
    // if (configuredType.isAbstract())
    // throw new
    // CloudGraphConfigurationException("a data graph definition within table '"
    // + table.getName() + "' has an abstract type (uri/name), "
    // + graph.getUri() + "#" + graph.getType() +
    // " - use a non abstract type");
    String qualifiedTableName = tableMapping.getQualifiedLogicalName();

    String qualifiedGraphName = dataGraphMapping.getQualifiedLogicalName();

    if (graphURIToTableMap.get(qualifiedGraphName) != null)
      throw new StoreMappingException("a data graph definition already exists within table '"
          + qualifiedTableName + "' for type (uri/name), " + dataGraphMapping.getGraph().getUri()
          + "#" + dataGraphMapping.getGraph().getType());
    graphURIToTableMap.put(qualifiedGraphName, dataGraphMapping.getTable());
    graphURIToGraphMap.put(qualifiedGraphName, dataGraphMapping);
    /*
     * Map<QName, PlasmaType> hierarchy = new HashMap<QName, PlasmaType>();
     * this.collectTypeHierarchy(configuredType, hierarchy);
     * 
     * for (PlasmaType type : hierarchy.values()) { qname =
     * type.getQualifiedName(); if (graphURIToTableMap.get(qname) != null) throw
     * new CloudGraphConfigurationException(
     * "a data graph definition already exists within table '" + table.getName()
     * + "' for type (uri/name), " + graph.getUri() + "#" + graph.getType());
     * graphURIToTableMap.put(qname, tableConfig); graphURIToGraphMap.put(qname,
     * dataGraphConfig); }
     */
  }

  private void unmapDataGraph(DataGraphMapping dataGraphConfig) {

    String qualifiedGraphName = dataGraphConfig.getQualifiedLogicalName();
    if (graphURIToTableMap.get(qualifiedGraphName) == null)
      throw new StoreMappingException("no data graph definition already exists within table '"
          + dataGraphConfig.getTable().getNamespaceQualifiedPhysicalName()
          + "' for type (uri/name), " + qualifiedGraphName);
    graphURIToTableMap.remove(qualifiedGraphName);
  }

  @SuppressWarnings("unchecked")
  private CloudGraphStoreMapping unmarshalConfig(InputStream stream, StoreMappingDataBinding binding) {
    try {

      CloudGraphStoreMapping result = (CloudGraphStoreMapping) binding.validate(stream);
      return result;
    } catch (UnmarshalException e) {
      throw new StoreMappingException(e);
    } catch (JAXBException e) {
      throw new StoreMappingException(e);
    }
  }

  @Override
  public void marshal(OutputStream stream) {
    try {
      StoreMappingDataBinding configBinding = new StoreMappingDataBinding(
          new DefaultValidationEventHandler());
      configBinding.marshal(this.config, stream);
    } catch (JAXBException e1) {
      throw new StoreMappingException(e1);
    } catch (SAXException e1) {
      throw new StoreMappingException(e1);
    }
  }

  public static MappingConfiguration getInstance() throws StoreMappingException {
    if (instance == null)
      initializeInstance();
    return instance;
  }

  private static synchronized void initializeInstance() {
    if (instance == null)
      instance = new StoreMapping();
  }

  @Override
  public ConfigProperties getConfigProperties() {
    return configProperties;
  }

  @Override
  public List<Property> getProperties() {
    return config.properties;
  }

  @Override
  public Property findProperty(String name) {
    return this.propertyNameToPropertyMap.get(name);
  }

  @Override
  public List<TableMapping> getStaticTables(StoreMappingContext context) {
    List<TableMapping> result = new ArrayList<>();
    for (TableMapping mapping : this.tableNameToTableMap.values()) {
      if (StaticTableMapping.class.isInstance(mapping))
        result.add(mapping);
    }

    return result;
  }

  @Override
  public List<TableMapping> getDynamicTables(StoreMappingContext context) {
    List<TableMapping> result = new ArrayList<>();
    for (TableMapping mapping : this.tableNameToTableMap.values()) {
      if (DynamicTableMapping.class.isInstance(mapping))
        result.add(mapping);
    }

    return result;
  }

  @Override
  public TableMapping findTable(QName typeName, StoreMappingContext context) {
    PlasmaType type = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(typeName.getNamespaceURI(),
        typeName.getLocalPart());
    return findTable(type, context);
  }

  @Override
  public TableMapping getTable(QName typeName, StoreMappingContext context) {
    TableMapping result = findTable(typeName, context);
    if (result == null)
      throw new StoreMappingException("no HTable configured for " + " graph URI '"
          + typeName.toString() + "'");
    return result;
  }

  @Override
  public TableMapping findTable(Type type, StoreMappingContext context) {
    lock.readLock().lock();
    try {
      PlasmaType plasmaType = (PlasmaType) type;
      String contextQualifiedName = DataGraphMapping.qualifiedLogicalNameFor(
          plasmaType.getQualifiedName(), context);
      TableMapping result = this.graphURIToTableMap.get(contextQualifiedName);
      if (result == null) {
        result = this.graphURIToTableMap.get(plasmaType.getQualifiedName().toString());
        if (result != null) {
          lock.readLock().unlock();
          lock.writeLock().lock();
          try {
            if (!StaticTableMapping.class.isAssignableFrom(result.getClass()))
              throw new IllegalStateException("expected static mapping for type, "
                  + plasmaType.getQualifiedName());
            if (!this.graphURIToTableMap.containsKey(contextQualifiedName)) {
              result = this.tableMappingFactory.createDynamicTableMapping(result.getTable(),
                  result.getMappingConfiguration(), context);
              this.graphURIToTableMap.put(contextQualifiedName, result);
            } else {
              result = this.graphURIToTableMap.get(contextQualifiedName);
            }
            lock.readLock().lock(); // downgrade
          } finally {
            lock.writeLock().unlock(); // still hold read
          }
        }
      }
      return result;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public TableMapping getTable(Type type, StoreMappingContext context) {
    TableMapping result = findTable(type, context);
    if (result == null)
      throw new StoreMappingException("no HTable configured for " + " graph URI '"
          + ((PlasmaType) type).getQualifiedName() + "'");
    return result;
  }

  private void collectTypeHierarchy(PlasmaType type, Map<QName, PlasmaType> map) {
    map.put(type.getQualifiedName(), type);
    // get ancestry
    collectBaseTypes(type, map);
    Collection<PlasmaType> coll = map.values();
    PlasmaType[] types = new PlasmaType[coll.size()];
    coll.toArray(types);

    // get all derived type for every ancestor
    for (int i = 0; i < types.length; i++) {
      PlasmaType baseType = types[i];
      collectSubTypes(baseType, map);
    }
  }

  private void collectBaseTypes(PlasmaType type, Map<QName, PlasmaType> map) {
    for (Type t : type.getBaseTypes()) {
      PlasmaType baseType = (PlasmaType) t;
      map.put(baseType.getQualifiedName(), baseType);
      collectBaseTypes(baseType, map);
    }
  }

  private void collectSubTypes(PlasmaType type, Map<QName, PlasmaType> map) {
    for (Type t : type.getSubTypes()) {
      PlasmaType subType = (PlasmaType) t;
      map.put(subType.getQualifiedName(), subType);
      collectSubTypes(subType, map);
    }
  }

  @Override
  public TableMapping findTableByQualifiedLogicalName(String qualifiedLogicaltableName,
      StoreMappingContext context) {
    lock.readLock().lock();
    try {
      String contextQualifiedName = qualifiedLogicaltableName;
      TableMapping result = this.tableNameToTableMap.get(contextQualifiedName);
      if (result == null) {
        result = this.tableNameToTableMap.get(contextQualifiedName);
        if (result == null) {
          String unqualifiedName = logicalTableNameFromQualifiedLogialTableName(
              contextQualifiedName, context);
          result = this.tableNameToTableMap.get(unqualifiedName);
          if (result != null) {
            lock.readLock().unlock();
            lock.writeLock().lock();
            try {
              if (!StaticTableMapping.class.isAssignableFrom(result.getClass()))
                throw new IllegalStateException("expected static mapping for type, "
                    + qualifiedLogicaltableName);
              // check again for other thread mod
              if (!this.tableNameToTableMap.containsKey(contextQualifiedName)) {
                result = this.tableMappingFactory.createDynamicTableMapping(result.getTable(),
                    result.getMappingConfiguration(), context);
                this.tableNameToTableMap.put(contextQualifiedName, result);
              } else {
                result = this.tableNameToTableMap.get(contextQualifiedName);
              }
              lock.readLock().lock(); // downgrade
            } finally {
              lock.writeLock().unlock(); // still hold read
            }
          }
        }
      }
      return result;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String qualifiedLogicalTableNameFromPhysicalTablePath(String namespace, String tableName,
      StoreMappingContext context) {
    String result = tableName;
    String pathPrefix = this.tableNamespaceRoot();
    if (pathPrefix != null && result.startsWith(pathPrefix)) {
      result = result.substring(pathPrefix.length());
    }
    if (result.startsWith(TableMapping.TABLE_PHYSICAL_NAME_DELIM))
      result = result.substring(TableMapping.TABLE_PHYSICAL_NAME_DELIM.length());
    return result;
  }

  private String logicalTableNameFromQualifiedLogialTableName(String tableName,
      StoreMappingContext context) {
    String result = tableName;
    if (context != null && context.hasTableVolumeName()) {
      String volumePrefix = context.getTableVolumeName();
      if (result.startsWith(volumePrefix) || result.startsWith(volumePrefix.toLowerCase()))
        result = result.substring(volumePrefix.length());
    }
    if (result.startsWith(TableMapping.TABLE_PHYSICAL_NAME_DELIM)) // FIXME:
                                                                   // should be
                                                                   // logical?
      result = result.substring(TableMapping.TABLE_PHYSICAL_NAME_DELIM.length());
    return result;
  }

  @Override
  public TableMapping getTableByQualifiedLogicalName(String qualifiedLogicalTableName,
      StoreMappingContext context) {
    TableMapping result = findTableByQualifiedLogicalName(qualifiedLogicalTableName, context);
    if (result != null) {
      return result;
    } else {
      throw new StoreMappingException("no table configured for '" + qualifiedLogicalTableName + "'");
    }
  }

  @Override
  public String getNamespaceQualifiedPhysicalTableName(QName typeName, StoreMappingContext context) {
    lock.readLock().lock();
    try {
      String contextQualifiedName = TableMapping.qualifiedLogicalNameFor(typeName, context);
      TableMapping result = this.graphURIToTableMap.get(contextQualifiedName);
      if (result == null) {
        result = this.graphURIToTableMap.get(typeName.toString());
        if (result == null)
          throw new StoreMappingException("no table configured for" + " CloudGraph '"
              + typeName.toString() + "'");
      }
      return result.getNamespaceQualifiedPhysicalName();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void addTable(TableMapping tableConfig) {
    lock.writeLock().lock();
    try {
      this.mapTable(tableConfig);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void addTableIfNotExists(TableMapping tableConfig) {
    lock.writeLock().lock();
    try {
      if ((this.tableNameToTableMap.get(tableConfig.getQualifiedLogicalName())) == null)
        this.mapTable(tableConfig);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void removeTable(TableMapping tableConfig) {
    lock.writeLock().lock();
    try {
      this.unmapTable(tableConfig);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void removeTableIfExists(TableMapping tableConfig) {
    lock.writeLock().lock();
    try {
      if ((this.tableNameToTableMap.get(tableConfig.getQualifiedLogicalName())) != null)
        this.unmapTable(tableConfig);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public DataGraphMapping findDataGraph(QName typeName, StoreMappingContext context) {
    lock.readLock().lock();
    try {
      String graphQualifiedName = DataGraphMapping.qualifiedLogicalNameFor(typeName, context);
      DataGraphMapping result = this.graphURIToGraphMap.get(graphQualifiedName);
      if (result == null) {
        result = this.graphURIToGraphMap.get(typeName.toString());
      }
      return result;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public DataGraphMapping getDataGraph(QName typeName, StoreMappingContext context) {
    DataGraphMapping result = this.findDataGraph(typeName, context);
    if (result == null)
      throw new StoreMappingException("no configured for" + " '" + typeName + "'");
    return result;
  }

  @Override
  public Charset getCharset() {
    return charset;
  }

  private Boolean uniqueChecksVar = null;

  @Override
  public boolean uniqueChecks() {
    if (uniqueChecksVar == null) {
      uniqueChecksVar = getTablePropertyBoolean(ConfigurationProperty.CLOUDGRAPH___UNIQUE___CHECKS,
          this.config.isUniqueChecks(), true);
    }
    return this.uniqueChecksVar.booleanValue();
  }

  private Boolean tombstoneRowsVar = null;

  @Override
  public boolean tombstoneRows() {
    if (tombstoneRowsVar == null) {
      tombstoneRowsVar = getTablePropertyBoolean(
          ConfigurationProperty.CLOUDGRAPH___TOMBSTONE___ROWS, this.config.isTombstoneRows(), true);
    }
    return this.tombstoneRowsVar.booleanValue();
  }

  private Boolean tombstoneRowsOverwriteableVar = null;

  @Override
  public boolean tombstoneRowsOverwriteable() {
    if (tombstoneRowsOverwriteableVar == null) {
      tombstoneRowsOverwriteableVar = getTablePropertyBoolean(
          ConfigurationProperty.CLOUDGRAPH___TOMBSTONE___ROWS___OVERWRITEABLE,
          this.config.isTombstoneRowsOverwriteable(), false);
    }
    return this.tombstoneRowsOverwriteableVar.booleanValue();
  }

  private String tableVolumeNameVar = null;

  public String tableVolumeName() {
    if (tableVolumeNameVar == null) {
      tableVolumeNameVar = getTablePropertyString(
          ConfigurationProperty.CLOUDGRAPH___TABLE___VOLUME___NAME,
          this.config.getTableVolumeName(), null);
    }
    return this.tableVolumeNameVar;
  }

  private String tableNamespaceRootVar = null;

  public String tableNamespaceRoot() {
    if (tableNamespaceRootVar == null) {
      tableNamespaceRootVar = getTablePropertyString(
          ConfigurationProperty.CLOUDGRAPH___TABLE___NAMESPACE___ROOT,
          this.config.getTableNamespaceRoot(), null);
    }
    return this.tableNamespaceRootVar;
  }

  private Boolean optimisticConcurrencyVar = null;

  @Override
  public boolean optimisticConcurrency() {
    if (optimisticConcurrencyVar == null) {
      optimisticConcurrencyVar = getTablePropertyBoolean(
          ConfigurationProperty.CLOUDGRAPH___CONCURRENCY___OPTIMISTIC___ENABLED,
          this.config.isOptimisticConcurrency(), true);
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

    String value = this.getConfigProperties().getString(prop.value());
    if (value != null) {
      result = value;
    } else {
      if (defaultValue != tableValue) { // default overridden, take the
        // table value
        result = tableValue;
      } else { // check for global config prop
        Property globalProp = this.findProperty(prop.value());
        if (globalProp != null) {
          result = globalProp.getValue();
        } else { // otherwise use schema default
          result = defaultValue;
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

    String value = this.getConfigProperties().getString(prop.value());
    if (value != null) {
      result = Boolean.valueOf(value);
    } else {
      if (defaultValue != tableValue) { // default overridden, take the
        // table value
        result = Boolean.valueOf(tableValue);
      } else { // check for global config prop
        Property globalProp = this.findProperty(prop.value());
        if (globalProp != null) {
          result = Boolean.valueOf(globalProp.getValue());
        } else { // otherwise use schema default
          result = Boolean.valueOf(defaultValue);
        }
      }
    }
    return result;
  }

}
