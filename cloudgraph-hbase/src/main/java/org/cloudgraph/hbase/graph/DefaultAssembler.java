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
package org.cloudgraph.hbase.graph;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.xml.namespace.QName;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.hbase.filter.GraphFetchColumnFilterAssembler;
import org.cloudgraph.hbase.io.CellValues;
import org.cloudgraph.hbase.io.EdgeOperation;
import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.io.RowReader;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.hbase.key.CompositeColumnKeyFactory;
import org.cloudgraph.hbase.service.HBaseDataConverter;
import org.cloudgraph.hbase.util.FilterUtil;
import org.cloudgraph.state.GraphRow;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.mapping.MappingConfiguration;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.collector.Selection;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.core.CoreNode;
import org.plasma.sdo.helper.PlasmaDataFactory;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.DataObject;
import commonj.sdo.Property;

/**
 * Supports both distributed and non-distributed graph assemblers by providing
 * default functionality.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public abstract class DefaultAssembler {

  private static Log log = LogFactory.getLog(DefaultAssembler.class);

  protected PlasmaType rootType;
  protected Map<PlasmaType, GraphColumnKeyFactory> keyFactories;
  protected PlasmaDataObject root;
  protected TableReader rootTableReader;

  // FIXME: table context obj?
  // FIXME can this go in RowReader ?
  protected DataGraphMapping graph;
  protected Charset charset;

  protected Selection selection;
  protected Timestamp snapshotDate;

  @SuppressWarnings("unused")
  private DefaultAssembler() {

  }

  /**
   * Constructor.
   * 
   * @param rootType
   *          the SDO root type for the result data graph
   * @param selection
   *          the selection properties for the graph to assemble.
   * @param snapshotDate
   *          the query snapshot date which is populated into every data object
   *          in the result data graph.
   */
  public DefaultAssembler(PlasmaType rootType, Selection selection, TableReader rootTableReader,
      Timestamp snapshotDate) {
    this.rootType = rootType;
    this.selection = selection;
    this.rootTableReader = rootTableReader;
    this.snapshotDate = snapshotDate;
    if (this.snapshotDate == null)
      throw new IllegalArgumentException("expected snapshotDate");

    // FIXME: table context obj?
    QName rootTypeQname = this.rootType.getQualifiedName();
    MappingConfiguration config = StoreMapping.getInstance();
    this.graph = config.getDataGraph(rootTypeQname, this.rootTableReader.getMappingContext());
    this.charset = config.getCharset();
    this.keyFactories = new HashMap<>();
    this.keyFactories.put(this.rootType, new CompositeColumnKeyFactory(this.rootType,
        this.rootTableReader.getMappingContext()));
  }

  /**
   * Returns the assembled data graph.
   */
  public PlasmaDataGraph getDataGraph() {
    return (PlasmaDataGraph) this.root.getDataGraph();
  }

  protected GraphColumnKeyFactory getKeyFactory(PlasmaType type) {
    GraphColumnKeyFactory result = this.keyFactories.get(type);
    if (result == null) {
      result = new CompositeColumnKeyFactory(type, this.rootTableReader.getMappingContext());
      this.keyFactories.put(type, result);
    }
    return result;
  }

  protected PlasmaDataObject createRoot(GraphColumnKeyFactory keyFactory, CellValues resultRow) {
    // build the graph
    PlasmaDataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.setId(resultRow.getRowKey());
    PlasmaDataObject rootObject = (PlasmaDataObject) dataGraph.createRootObject(this.rootType);
    CoreNode rootNode = (CoreNode) rootObject;

    // add concurrency fields
    rootNode.setValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP, snapshotDate);
    rootNode.getValueObject().put(CloudGraphConstants.GRAPH_NODE_THREAD_NAME,
        Thread.currentThread().getName());

    byte[] uuidQual = keyFactory.createColumnKey(this.rootType, EntityMetaKey.UUID);
    byte[] rootUuid = resultRow.getColumnValue(
        Bytes.toBytes(this.rootTableReader.getTableConfig().getDataColumnFamilyName()), uuidQual);
    if (rootUuid == null)
      throw new GraphServiceException("expected column: "
          + this.rootTableReader.getTableConfig().getDataColumnFamilyName() + ":"
          + EntityMetaKey.UUID);

    // need to reconstruct the original graph, so need original UUID
    String uuidStr = null;
    uuidStr = new String(rootUuid, this.rootTableReader.getTableConfig().getCharset());
    UUID uuid = UUID.fromString(uuidStr);
    rootObject.resetUUID(uuid);
    return rootObject;
  }

  /**
   * Creates contained child data object with the same type as the given
   * containment property or of a specific given sub-type as determined by
   * querying the entity state.
   * 
   * @param source
   *          the container data object
   * @param prop
   *          the containment property
   * @param uuid
   *          the uuid for the new child
   * @param subType
   *          the specific (sub) type for the new child which may differ from
   *          the type for the containment property
   * @return the new child data object
   * @throws IOException
   */
  protected PlasmaDataObject createChild(PlasmaDataObject source, PlasmaProperty prop, UUID uuid,
      PlasmaType subType) throws IOException {
    if (log.isDebugEnabled())
      log.debug("creating data object (" + uuid + ") type:  " + subType.toString());
    PlasmaDataObject child = (PlasmaDataObject) source.createDataObject(prop, subType);
    child.resetUUID(uuid);
    CoreNode childNode = ((CoreNode) child);
    childNode.setValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP, snapshotDate);
    childNode
        .setValue(CloudGraphConstants.GRAPH_NODE_THREAD_NAME, Thread.currentThread().getName());
    childNode.setValue(CloudGraphConstants.SEQUENCE, CloudGraphConstants.ROOT_SEQUENCE);
    return child;
  }

  protected PlasmaDataObject createChild(long targetSequence, EdgeOperation collection,
      DataObject source, PlasmaProperty sourceProperty, RowReader rowReader,
      DataGraphMapping graphConfig) throws IOException {
    PlasmaType subType = collection.getSubType();
    if (subType == null)
      subType = collection.getBaseType();

    if (log.isDebugEnabled())
      log.debug("creating data object type:  " + subType.toString());
    PlasmaDataObject child = (PlasmaDataObject) source.createDataObject(sourceProperty, subType);
    CoreNode childNode = ((CoreNode) child);
    childNode.setValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP, snapshotDate);
    childNode
        .setValue(CloudGraphConstants.GRAPH_NODE_THREAD_NAME, Thread.currentThread().getName());
    childNode.setValue(CloudGraphConstants.SEQUENCE, targetSequence);

    return child;
  }

  protected PlasmaDataObject createChild(UUID uuid, PlasmaType childType, long targetSequence,
      DataObject source, PlasmaProperty sourceProperty) throws IOException {
    if (log.isDebugEnabled())
      log.debug("creating data object type:  " + childType.toString());
    PlasmaDataObject child = (PlasmaDataObject) source.createDataObject(sourceProperty, childType);
    CoreNode childNode = ((CoreNode) child);
    childNode.setValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP, snapshotDate);
    childNode
        .setValue(CloudGraphConstants.GRAPH_NODE_THREAD_NAME, Thread.currentThread().getName());
    childNode.setValue(CloudGraphConstants.SEQUENCE, targetSequence);
    child.resetUUID(uuid);
    return child;
  }

  protected void assembleUUID(PlasmaDataObject target, long targetSequence, EdgeReader edgeReader,
      RowReader rowReader) throws IOException {
    byte[] uuidValue = this.getMetaDataColumnValue((PlasmaType) target.getType(), targetSequence,
        EntityMetaKey.UUID, rowReader.getTableReader().getTableConfig(), rowReader);
    String uuidStr = null;
    uuidStr = new String(uuidValue, rowReader.getTableReader().getTableConfig().getCharset());
    UUID uuid = UUID.fromString(uuidStr);
    target.resetUUID(uuid);
  }

  protected UUID fetchUUID(PlasmaType type, long targetSequence, EdgeReader edgeReader,
      RowReader rowReader) throws IOException {
    byte[] uuidValue = this.getMetaDataColumnValue(type, targetSequence, EntityMetaKey.UUID,
        rowReader.getTableReader().getTableConfig(), rowReader);
    String uuidStr = null;
    uuidStr = new String(uuidValue, rowReader.getTableReader().getTableConfig().getCharset());
    UUID uuid = null;
    if (uuidStr.length() == 22) {
      byte[] bytes = Base64.decodeBase64(uuidStr);
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      uuid = new UUID(bb.getLong(), bb.getLong());
    } else {
      uuid = UUID.fromString(uuidStr);
    }
    return uuid;
  }

  protected UUID findRootUUID(TableReader childTableReader, GraphColumnKeyFactory keyFactory,
      PlasmaType subType, CellValues childResult) {

    byte[] uuidQual = keyFactory.createColumnKey(subType, EntityMetaKey.UUID);
    byte[] rootUuid = childResult.getColumnValue(childTableReader.getTableConfig()
        .getDataColumnFamilyNameBytes(), uuidQual);
    if (rootUuid != null) {
      String uuidStr = null;
      uuidStr = new String(rootUuid, childTableReader.getTableConfig().getCharset());
      UUID uuid = null;
      if (uuidStr.length() == 22) {
        byte[] bytes = Base64.decodeBase64(uuidStr);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        uuid = new UUID(bb.getLong(), bb.getLong());
      } else {
        uuid = UUID.fromString(uuidStr);
      }
      return uuid;
    }
    return null;
  }

  protected UUID fetchRootUUID(TableReader childTableReader, GraphColumnKeyFactory keyFactory,
      PlasmaType subType, CellValues childResult) {

    byte[] uuidQual = keyFactory.createColumnKey(subType, EntityMetaKey.UUID);
    byte[] rootUuid = childResult.getColumnValue(childTableReader.getTableConfig()
        .getDataColumnFamilyNameBytes(), uuidQual);
    if (rootUuid == null)
      throw new GraphServiceException("expected column: " + Bytes.toString(uuidQual) + " for row '"
          + childResult.getRowKey() + "' in table: " + childTableReader.getTableConfig().getName());
    String uuidStr = new String(rootUuid, childTableReader.getTableConfig().getCharset());
    UUID uuid = null;
    if (uuidStr.length() == 22) {
      byte[] bytes = Base64.decodeBase64(uuidStr);
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      uuid = new UUID(bb.getLong(), bb.getLong());
    } else {
      uuid = UUID.fromString(uuidStr);
    }
    return uuid;
  }

  protected PlasmaType fetchRootType(TableReader childTableReader,
      GraphColumnKeyFactory keyFactory, PlasmaType subType, CellValues childResult)
      throws IOException {
    byte[] typeQual = keyFactory.createColumnKey(subType, EntityMetaKey.TYPE);
    byte[] rootType = childResult.getColumnValue(childTableReader.getTableConfig()
        .getDataColumnFamilyNameBytes(), typeQual);
    if (rootType == null)
      throw new GraphServiceException("expected column: " + Bytes.toString(typeQual) + " for row '"
          + childResult.getRowKey() + "' in table: " + childTableReader.getTableConfig().getName());
    String[] tokens = Bytes.toString(rootType).split(GraphRow.ROOT_TYPE_DELIM);
    PlasmaType result = (PlasmaType) PlasmaTypeHelper.INSTANCE.findTypeByPhysicalName(tokens[0],
        tokens[1]);
    if (result == null)
      throw new GraphServiceException("no type found for '" + Bytes.toString(rootType) + "'");
    return result;
  }

  protected PlasmaType findRootType(TableReader childTableReader, GraphColumnKeyFactory keyFactory,
      PlasmaType subType, CellValues childResult) throws IOException {
    byte[] typeQual = keyFactory.createColumnKey(subType, EntityMetaKey.TYPE);
    byte[] rootType = childResult.getColumnValue(childTableReader.getTableConfig()
        .getDataColumnFamilyNameBytes(), typeQual);
    if (rootType != null) {
      String[] tokens = Bytes.toString(rootType).split(GraphRow.ROOT_TYPE_DELIM);
      PlasmaType result = (PlasmaType) PlasmaTypeHelper.INSTANCE.findTypeByPhysicalName(tokens[0],
          tokens[1]);
      if (result == null)
        throw new GraphServiceException("no type found for '" + Bytes.toString(rootType) + "'");
      return result;
    }
    return null;
  }

  protected PlasmaType fetchType(PlasmaType type, long targetSequence, EdgeReader edgeReader,
      RowReader rowReader) throws IOException {
    byte[] typeValue = this.getMetaDataColumnValue(type, targetSequence, EntityMetaKey.TYPE,
        rowReader.getTableReader().getTableConfig(), rowReader);
    PlasmaType result = rowReader.decodeType(typeValue);
    if (result == null)
      throw new GraphServiceException("no type found for '" + Bytes.toString(typeValue)
          + "' in table: " + rowReader.getTableReader().getTableConfig().getName());
    return result;
  }

  protected void assembleData(PlasmaDataObject target, long targetSequence, Set<Property> props,
      RowReader rowReader) throws IOException {
    CoreNode targetDataNode = (CoreNode) target;
    PlasmaType targetType = (PlasmaType) target.getType();

    // add concurrency fields
    targetDataNode.setValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP, snapshotDate);
    TableMapping tableConfig = rowReader.getTableReader().getTableConfig();
    // data props
    for (Property p : props) {
      PlasmaProperty prop = (PlasmaProperty) p;
      if (!prop.getType().isDataType())
        continue;

      byte[] keyValue = getDataColumnValue(targetType, prop, targetSequence, tableConfig, rowReader);

      if (keyValue == null || keyValue.length == 0) {
        continue; // zero length can happen on modification or delete as
        // we keep cell history
      }

      Object value = HBaseDataConverter.INSTANCE.fromBytes(prop, keyValue);

      if (log.isDebugEnabled())
        log.debug("set: (" + prop.getName() + ") " + String.valueOf(value));

      if (!prop.isReadOnly()) {
        target.set(prop, value);
      } else {
        targetDataNode.setValue(prop.getName(), value);
      }
    }
  }

  protected byte[] getMetaDataColumnValue(PlasmaType type, long sequence, EntityMetaKey keyField,
      TableMapping tableConfig, RowReader rowReader) throws IOException {
    byte[] family = tableConfig.getDataColumnFamilyNameBytes();
    byte[] qualifier = rowReader.getColumnKeyFactory().createColumnKey(type, sequence, keyField);

    if (!rowReader.getRow().containsColumn(family, qualifier)) {
      throw new IllegalArgumentException("expected value for qualifier, "
          + Bytes.toString(qualifier));
    }

    return rowReader.getRow().getColumnValue(family, qualifier);
  }

  protected byte[] getMetaDataColumnValue(PlasmaType type, EntityMetaKey keyField,
      TableMapping tableConfig, CellValues result, RowReader rowReader) throws IOException {
    byte[] family = tableConfig.getDataColumnFamilyNameBytes();
    byte[] qualifier = rowReader.getColumnKeyFactory().createColumnKey(type, keyField);

    if (!result.containsColumn(family, qualifier)) {
      throw new IllegalArgumentException("expected value for qualifier, "
          + Bytes.toString(qualifier));
    }

    return result.getColumnValue(family, qualifier);
  }

  protected byte[] findMetaDataColumnValue(PlasmaType type, PlasmaProperty property, long sequence,
      EntityMetaKey keyField, TableMapping tableConfig, RowReader rowReader) throws IOException {
    byte[] family = tableConfig.getDataColumnFamilyNameBytes();

    byte[] qualifier = rowReader.getColumnKeyFactory().createColumnKey(type, sequence, property,
        keyField);

    if (!rowReader.getRow().containsColumn(family, qualifier)) {
      if (log.isDebugEnabled()) {
        String qualifierStr = Bytes.toString(qualifier);
        log.debug("qualifier not found: " + qualifierStr + " - continuing...");
      }
      return null;
    }

    return rowReader.getRow().getColumnValue(family, qualifier);
  }

  protected byte[] findMetaDataColumnValue(PlasmaType type, long sequence, EntityMetaKey keyField,
      TableMapping tableConfig, RowReader rowReader) throws IOException {
    byte[] family = tableConfig.getDataColumnFamilyNameBytes();

    byte[] qualifier = rowReader.getColumnKeyFactory().createColumnKey(type, sequence, keyField);

    if (!rowReader.getRow().containsColumn(family, qualifier)) {
      if (log.isDebugEnabled()) {
        String qualifierStr = Bytes.toString(qualifier);
        log.debug("qualifier not found: " + qualifierStr + " - continuing...");
      }
      return null;
    }

    return rowReader.getRow().getColumnValue(family, qualifier);
  }

  /**
   * Returns a value for the given property from the given row reader by
   * generating a column qualifier based on column key model configurations
   * settings, graph state information and other factors. Returns null if the
   * qualifier does not exist.
   * 
   * @param target
   *          the data object
   * @param prop
   *          the property
   * @param typeSequenceNum
   *          the graph row specific sequence number for the given type for a
   *          data object
   * @param tableConfig
   *          the table configuration
   * @param rowReader
   *          the row reader
   * @return a value for the given property from the given row reader by
   *         generating a column qualifier based on column key model
   *         configurations settings, graph state information and other factors.
   * @throws IOException
   *           if a remote or network exception occurs.
   */
  protected byte[] getDataColumnValue(PlasmaType type, PlasmaProperty prop, long typeSequenceNum,
      TableMapping tableConfig, RowReader rowReader) throws IOException {
    byte[] family = tableConfig.getDataColumnFamilyNameBytes();

    byte[] qualifier = null;
    if (typeSequenceNum > 0)
      qualifier = rowReader.getColumnKeyFactory().createColumnKey(type, typeSequenceNum, prop);
    else
      qualifier = rowReader.getColumnKeyFactory().createColumnKey(type, prop);

    if (!rowReader.getRow().containsColumn(family, qualifier)) {
      if (log.isDebugEnabled()) {
        String qualifierStr = Bytes.toString(qualifier);
        log.debug("qualifier not found: " + qualifierStr + " - continuing...");
      }
      return null;
    }

    return rowReader.getRow().getColumnValue(family, qualifier);
  }

  protected byte[] getDataColumnValue(PlasmaDataObject target, PlasmaProperty prop,
      TableMapping tableConfig, RowReader rowReader) throws IOException {
    byte[] family = tableConfig.getDataColumnFamilyNameBytes();

    byte[] qualifier = rowReader.getColumnKeyFactory().createColumnKey(
        (PlasmaType) target.getType(), prop);

    if (!rowReader.getRow().containsColumn(family, qualifier)) {
      if (log.isDebugEnabled()) {
        String qualifierStr = Bytes.toString(qualifier);
        log.debug("qualifier not found: " + qualifierStr + " - continuing...");
      }
      return null;
    }

    return rowReader.getRow().getColumnValue(family, qualifier);
  }

  protected Set<Property> getProperties(PlasmaDataObject target, PlasmaDataObject source,
      PlasmaProperty sourceProperty, int level) {
    return getProperties((PlasmaType) target.getType(), source, sourceProperty, level);
  }

  protected Set<Property> getProperties(PlasmaType type, PlasmaDataObject source,
      PlasmaProperty sourceProperty, int level) {
    Set<Property> props;
    if (sourceProperty != null) {
      // props = this.selection.getInheritedProperties(target.getType(),
      // sourceProperty, level);
      props = this.selection.getInheritedProperties(type, level);
      if (props.size() == 0) {
        if (log.isDebugEnabled())
          log.debug("no properties for " + type + " at level: " + level + " for source edge, "
              + sourceProperty.toString() + " - aborting traversal");
      }
    } else {
      props = this.selection.getInheritedProperties(type, level);
      if (props.size() == 0) {
        if (log.isDebugEnabled())
          log.debug("no properties for " + type + " at level: " + level + " - aborting traversal");
      }
    }
    return props;
  }

  /**
   * Associates the source data object with the target as a non-containment
   * reference.
   * 
   * @param target
   *          the data object target
   * @param source
   *          the data object source
   * @param sourceProperty
   *          the reference property
   * @throws IllegalStateException
   *           if the target data object does not have a container
   */
  @SuppressWarnings("unchecked")
  protected void link(PlasmaDataObject target, PlasmaDataObject source, Property sourceProperty) {
    if (log.isDebugEnabled())
      log.debug("linking source/target (" + source.toString() + "->(" + target.toString());

    if (sourceProperty.isMany()) {
      PlasmaProperty opposite = (PlasmaProperty) sourceProperty.getOpposite();
      if (opposite != null && !opposite.isMany() && target.isSet(opposite)) {
        PlasmaDataObject existingOpposite = (PlasmaDataObject) target.get(opposite);
        if (existingOpposite != null) {
          if (log.isDebugEnabled())
            log.debug("found existing opposite value (" + existingOpposite.toString()
                + ") for source/target " + sourceProperty.toString() + "->" + target.toString()
                + " - no link created");
          return;
        }
      }

      List<DataObject> list = source.getList(sourceProperty);
      if (list == null)
        list = new ArrayList<DataObject>();
      if (!list.contains(target)) {
        // check if any existing list members already have the opposite
        // property set
        for (DataObject existing : list) {
          if (opposite != null && !opposite.isMany() && existing.isSet(opposite)) {
            PlasmaDataObject existingOpposite = (PlasmaDataObject) existing.get(opposite);
            if (existingOpposite != null) {
              if (log.isDebugEnabled())
                log.debug("(2)found existing opposite value (" + existingOpposite.toString()
                    + ") for source/target " + sourceProperty.toString() + "->" + target.toString()
                    + " - no link created");
              return;
            }
          }
        }
        if (log.isDebugEnabled())
          log.debug("adding target " + source.toString() + "." + sourceProperty.getName() + "->"
              + target.toString());
        if (target.getContainer() == null) {
          if (source.getDataGraph().getRootObject().equals(target)) {
            log.warn("linking root object, " + target.toString() + " to source, "
                + source.toString());
          } else
            throw new IllegalStateException("the given target has no container: "
                + target.toString());
        }
        list.add(target);
        source.setList(sourceProperty, list);
      }
    } else {
      PlasmaDataObject existing = (PlasmaDataObject) source.get(sourceProperty);
      if (existing == null) {
        if (target.getContainer() == null) {
          if (source.getDataGraph().getRootObject().equals(target)) {
            log.warn("linking root object, " + target.toString() + " to source, "
                + source.toString());
          } else
            throw new IllegalStateException("the given target has no container: "
                + target.toString());
        }
        source.set(sourceProperty, target);
      } else if (!existing.equals(target))
        if (log.isDebugEnabled())
          log.debug("found existing value (" + existing.toString()
              + ") while creating source/target link " + source.toString() + "."
              + sourceProperty.toString() + "->" + target.toString());
    }
  }

  /**
   * Returns the selection graph as a single result.
   * 
   * @param rowKey
   *          the row key
   * @param tableReader
   *          the table reader
   * @param dataObject
   *          the
   * @return the selection graph as a single result.
   * @throws IOException
   *           if a remote or network exception occurs.
   * @see GraphFetchColumnFilterAssembler
   */
  protected CellValues fetchGraph(byte[] rowKey, TableReader tableReader, PlasmaType type)
      throws IOException {
    Get row = new Get(rowKey);
    FilterList rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    row.setFilter(rootFilter);
    GraphFetchColumnFilterAssembler columnFilterAssembler = new GraphFetchColumnFilterAssembler(
        this.selection, type, this.rootTableReader.getMappingContext());
    rootFilter.addFilter(columnFilterAssembler.getFilter());
    long before = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("filter: " + FilterUtil.printFilterTree(rootFilter));
    if (log.isDebugEnabled())
      log.debug("executing get...");
    Result result = tableReader.getTable().get(row);
    if (result == null || result.isEmpty())
      throw new GraphServiceException("expected result from table "
          + tableReader.getTableConfig().getName() + " for row '" + new String(rowKey) + "'");
    long after = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("assembled 1 results (" + String.valueOf(after - before) + ")");
    return new CellValues(result);
  }
}
