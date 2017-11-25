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
package org.cloudgraph.hbase.mutation;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.io.DistributedWriter;
import org.cloudgraph.hbase.io.RowWriter;
import org.cloudgraph.hbase.service.HBaseDataConverter;
import org.cloudgraph.hbase.service.ServiceContext;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.mapping.UserDefinedRowKeyFieldMapping;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaEdge;
import org.plasma.sdo.PlasmaNode;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.RequiredPropertyException;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.core.CoreDataObject;
import org.plasma.sdo.core.NullValue;
import org.plasma.sdo.core.SnapshotMap;
import org.plasma.sdo.helper.DataConverter;
import org.plasma.sdo.profile.ConcurrencyType;
import org.plasma.sdo.profile.ConcurrentDataFlavor;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;
import commonj.sdo.Property;

abstract class DefaultMutation {
  private static Log log = LogFactory.getLog(DefaultMutation.class);
  private static HashMap<String, DataObject> EMPTY_EDGE_MAP = new HashMap<String, DataObject>();
  protected SnapshotMap snapshotMap;
  protected String username;
  protected ServiceContext context;

  public DefaultMutation(ServiceContext context, SnapshotMap snapshotMap, String username) {
    super();
    this.snapshotMap = snapshotMap;
    this.username = username;
    this.context = context;
  }

  protected HashMap<String, DataObject> getOldEdgeMap(Object oldValue, Property property) {
    HashMap<String, DataObject> result = EMPTY_EDGE_MAP;
    if (!(oldValue instanceof NullValue)) {
      if (oldValue instanceof List) {
        @SuppressWarnings("unchecked")
        List<DataObject> oldValueList = (List<DataObject>) oldValue;
        result = new HashMap<String, DataObject>(oldValueList.size());
        for (DataObject dataObject : oldValueList)
          result.put(((PlasmaDataObject) dataObject).getUUIDAsString(), dataObject);
      } else {
        PlasmaDataObject oldOpposite = (PlasmaDataObject) oldValue;
        result = new HashMap<String, DataObject>(1);
        result.put(oldOpposite.getUUIDAsString(), oldOpposite);
      }
    }
    return result;
  }

  // protected byte[] createEdgeValueBytes(PlasmaNode dataNode,
  // List<PlasmaEdge> edges, RowWriter rowWriter)
  // throws IOException {
  // String valueStr = rowWriter.getGraphState().marshalEdges(dataNode,
  // edges);
  // return Bytes.toBytes(valueStr);
  // }

  // protected void addRowKeys(
  // PlasmaDataObject dataObject,
  // PlasmaNode dataNode,
  // Property property,
  // List <PlasmaEdge> edges,
  // DistributedWriter graphWriter,
  // TableWriter tableWriter,
  // RowWriter rowWriter) throws IOException
  // {
  // PlasmaType dataObjectType = (PlasmaType)dataObject.getType();
  //
  // boolean thisTypeBound = CloudGraphConfig.getInstance().findTable(
  // dataObjectType.getQualifiedName()) != null;
  //
  // for (PlasmaEdge edge : edges) {
  //
  // PlasmaDataObject opposite = edge.getOpposite(dataNode).getDataObject();
  // //edge.getDirection()
  //
  // PlasmaType oppositeType = (PlasmaType)opposite.getType();
  // boolean oppositeTypeBound = CloudGraphConfig.getInstance().findTable(
  // oppositeType.getQualifiedName()) != null;
  //
  // RowWriter oppositeRowWriter = graphWriter.findRowWriter(opposite);
  // if (oppositeRowWriter == null) {
  // oppositeRowWriter = graphWriter.createRowWriter(opposite);
  // }
  // TableWriter oppositeTableWriter = oppositeRowWriter.getTableWriter();
  //
  // // maps opposite UUID to its row key
  // // in the state for this row
  // if (oppositeTypeBound) {
  // rowWriter.getSequenceMapping().addRowKey(opposite,
  // oppositeTableWriter.getTableConfig(),
  // oppositeRowWriter.getRowKey());
  // if (log.isDebugEnabled())
  // log.debug("adding row key for opposite data object ("+opposite+") to this data-object state, "
  // + dataObject);
  // }
  // else {
  // if (log.isDebugEnabled())
  // log.debug("ignoring row key for unbound opposite data object, "
  // + opposite.toString());
  // }
  //
  // // Maps this DO uuid to current row key in opposite row
  // // If this data object is not "bound" to a
  // // table, disregard as it will have no opposite row
  // // but will be contained within this row
  // Property oppositeProperty = property.getOpposite();
  // if (oppositeProperty != null && thisTypeBound) {
  // ChangeSummary changeSummary = dataObject.getChangeSummary();
  // if (changeSummary.isCreated(opposite) ||
  // changeSummary.isModified(opposite)) {
  // oppositeRowWriter.getSequenceMapping().addRowKey(dataObject,
  // tableWriter.getTableConfig(),
  // rowWriter.getRowKey());
  // if (log.isDebugEnabled())
  // log.debug("adding row key for this data object ("+dataObject+") to opposite data-object state, "
  // + opposite + " for property, " + property);
  // }
  // // Otherwise we are just adding to the state for a DO who's ref property
  // // will never get updated
  // // And is this call even necessary as if the opposite's property IS
  // modified
  // // it will get updated above.
  // }
  // else {
  // if (log.isDebugEnabled()) {
  // if (oppositeProperty == null)
  // log.debug("ignoring row key for opposite data-object state for property, "
  // + property.toString() + " as no opposite property exists");
  // else
  // log.debug("ignoring row key for opposite data-object state for property, "
  // + property.toString() + " as this type is not bound");
  // }
  //
  // }
  // }
  // }

  /**
   * Screens out edges that are not owned by the given data node, but still may
   * be present.
   */
  @Deprecated
  protected List<PlasmaEdge> findOwnedEdges(PlasmaNode dataNode, Property property,
      List<PlasmaEdge> edges, DistributedWriter graphWriter, RowWriter rowWriter)
      throws IOException {
    List<PlasmaEdge> result = new ArrayList<PlasmaEdge>();
    for (PlasmaEdge edge : edges) {
      PlasmaDataObject opposite = edge.getOpposite(dataNode).getDataObject();
      PlasmaType oppositeType = (PlasmaType) opposite.getType();
      boolean oppositeTypeBound = StoreMapping.getInstance().findTable(
          oppositeType.getQualifiedName()) != null;
      RowWriter oppositeRowWriter = graphWriter.findRowWriter(opposite);
      if (oppositeRowWriter == null) {
        oppositeRowWriter = graphWriter.createRowWriter(opposite);
      }

      // If the opposite not bound to a table and
      // it is already linked within another row,
      // don't write the edge. This graph does not
      // own it.
      if (oppositeTypeBound || oppositeRowWriter.equals(rowWriter)) {
        result.add(edge);
      } else {
        if (log.isDebugEnabled())
          log.debug("ignoring non-owned edge for property, " + property.toString());
      }
    }
    return result;
  }

  protected void updateCell(RowWriter rowContext, PlasmaDataObject dataObject, long sequence,
      Property property, byte[] value) throws IOException {
    PlasmaProperty prop = (PlasmaProperty) property;
    byte[] qualifier = rowContext.getColumnKeyFactory().createColumnKey(
        (PlasmaType) dataObject.getType(), sequence, prop);
    TableMapping table = rowContext.getTableWriter().getTableConfig();
    if (log.isDebugEnabled()) {
      if (prop.getType().isDataType()) {
        Object objectValue = HBaseDataConverter.INSTANCE.fromBytes(prop, value);
        log.debug("setting " + property + " / " + table.getName() + "."
            + new String(qualifier, table.getCharset()) + " = '" + String.valueOf(objectValue)
            + "'");
      } else {
        log.debug("setting " + property + " / " + table.getName() + "."
            + new String(qualifier, table.getCharset()) + " = '"
            + new String(value, table.getCharset()) + "'");
      }
    }
    rowContext.getRow().add(table.getDataColumnFamilyNameBytes(), qualifier, value);
  }

  /**
   * Removes all versions of the associated cell.
   * 
   * @param rowContext
   *          row writer
   * @param dataObject
   *          the data object
   * @param property
   * @throws IOException
   */
  protected void deleteDataCell(RowWriter rowContext, PlasmaDataObject dataObject, long sequence,
      PlasmaProperty property) throws IOException {
    byte[] qualifier = rowContext.getColumnKeyFactory().createColumnKey(dataObject, sequence,
        property);
    TableMapping table = rowContext.getTableWriter().getTableConfig();
    if (log.isDebugEnabled())
      log.debug("deleting " + property + " / " + table.getName() + "."
          + new String(qualifier, table.getCharset()));
    rowContext.getRowDelete().addColumns(table.getDataColumnFamilyNameBytes(), qualifier);
  }

  protected void setOrigination(PlasmaDataObject dataObject, PlasmaType type) {
    // FIXME - could be a reference to a user
    Property originationUserProperty = type.findProperty(ConcurrencyType.origination,
        ConcurrentDataFlavor.user);
    if (originationUserProperty != null) {
      if (!originationUserProperty.isReadOnly())
        dataObject.set(originationUserProperty, username);
      else
        ((CoreDataObject) dataObject).setValue(originationUserProperty.getName(), username); // FIXME:
      // bypassing
      // readonly
      // modification
      // detection
    } // else if (log.isDebugEnabled())
      // log.debug(
      // "could not find origination (username) property for type, " +
      // type.getURI() + "#" + type.getName());

    Property originationTimestampProperty = type.findProperty(ConcurrencyType.origination,
        ConcurrentDataFlavor.time);
    if (originationTimestampProperty != null) {
      Date dateSnapshot = new Date(this.snapshotMap.getSnapshotDate().getTime());
      Object snapshot = DataConverter.INSTANCE.convert(originationTimestampProperty.getType(),
          dateSnapshot);
      if (!originationTimestampProperty.isReadOnly())
        dataObject.set(originationTimestampProperty, snapshot);
      else
        ((CoreDataObject) dataObject).setValue(originationTimestampProperty.getName(), snapshot); // FIXME:
      // bypassing
      // readonly
      // modification
      // detection
    } // else if (log.isDebugEnabled())
      // log.debug("could not find origination date property for type, " +
      // type.getURI() + "#" + type.getName());
  }

  // protected void updateKeys(PlasmaDataObject dataObject, PlasmaType type,
  // RowWriter rowWriter) throws IOException {
  // UUID uuid = ((CoreDataObject) dataObject).getUUID();
  // if (uuid == null)
  // throw new GraphServiceException("expected UUID for created entity '" +
  // type.getName() + "'");
  // List<Property> pkList = type.findProperties(KeyType.primary);
  // if (pkList == null || pkList.size() == 0)
  // return; // don't care for NOSQL services
  //
  // for (Property pkp : pkList) {
  // if (!pkp.getType().isDataType())
  // continue; // noop for non-data pks
  //
  // PlasmaProperty targetPriKeyProperty = (PlasmaProperty) pkp;
  // Object pk = dataObject.get(targetPriKeyProperty);
  // if (pk == null) {
  // DataFlavor dataFlavor = targetPriKeyProperty.getDataFlavor();
  // switch (dataFlavor) {
  // case integral:
  // if (log.isDebugEnabled()) {
  // log.debug("getting seq-num for " + type.getName());
  // }
  // pk = rowWriter.getGraphState().findSequence(dataObject);
  // if (pk == null)
  // pk = rowWriter.getGraphState().addSequence(dataObject);
  // pk = DataConverter.INSTANCE.convert(targetPriKeyProperty.getType(), pk);
  // byte[] pkBytes =
  // HBaseDataConverter.INSTANCE.toBytes(targetPriKeyProperty, pk);
  // this.updateCell(rowWriter, dataObject, targetPriKeyProperty, pkBytes);
  // ((CoreDataObject) dataObject).setValue(targetPriKeyProperty.getName(),
  // pk); // FIXME:
  // // bypassing
  // // modification
  // // detection
  // // on
  // // pri-key
  // break;
  // default:
  // throw new DataAccessException("found null primary key property '" +
  // targetPriKeyProperty.getName()
  // + "' for type, " + type.getURI() + "#" + type.getName());
  // }
  // } else {
  // byte[] pkBytes =
  // HBaseDataConverter.INSTANCE.toBytes(targetPriKeyProperty, pk);
  // this.updateCell(rowWriter, dataObject, targetPriKeyProperty, pkBytes);
  // }
  // if (log.isDebugEnabled()) {
  // log.debug("mapping UUID '" + uuid + "' to pk (" + String.valueOf(pk) +
  // ")");
  // }
  // snapshotMap.put(uuid, new PropertyPair(targetPriKeyProperty, pk));
  // }
  // }

  protected void updateOrigination(PlasmaDataObject dataObject, long sequence, PlasmaType type,
      RowWriter rowContext) throws IOException {
    // FIXME - could be a reference to a user
    Property originationUserProperty = type.findProperty(ConcurrencyType.origination,
        ConcurrentDataFlavor.user);
    if (originationUserProperty != null) {
      if (!originationUserProperty.isReadOnly())
        dataObject.set(originationUserProperty, username);
      else
        ((CoreDataObject) dataObject).setValue(originationUserProperty.getName(), username); // FIXME:
      // bypassing
      // readonly
      // modification
      // detection
    } // else if (log.isDebugEnabled())
      // log.debug(
      // "could not find origination (username) property for type, " +
      // type.getURI() + "#" + type.getName());

    PlasmaProperty originationTimestampProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.origination, ConcurrentDataFlavor.time);
    if (originationTimestampProperty != null) {
      Date dateSnapshot = new Date(this.snapshotMap.getSnapshotDate().getTime());
      Object snapshot = DataConverter.INSTANCE.convert(originationTimestampProperty.getType(),
          dateSnapshot);
      byte[] bytes = HBaseDataConverter.INSTANCE.toBytes(originationTimestampProperty, snapshot);
      rowContext.writeRowData(dataObject, sequence, originationTimestampProperty, bytes);
    } // else if (log.isDebugEnabled())
      // log.debug("could not find origination date property for type, " +
      // type + "#" + type.getName());
  }

  protected void setOptimistic(PlasmaDataObject dataObject, PlasmaType type, Timestamp snapshotDate) {
    PlasmaProperty concurrencyUserProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.optimistic, ConcurrentDataFlavor.user);
    if (concurrencyUserProperty == null) {
      // if (log.isDebugEnabled())
      // log.debug("could not find optimistic concurrency (username) property for type, "
      // + type.getURI() + "#"
      // + type.getName());
    } else {
      if (!concurrencyUserProperty.isReadOnly())
        dataObject.set(concurrencyUserProperty, username);
      else
        ((CoreDataObject) dataObject).setValue(concurrencyUserProperty.getName(), username);
    }

    PlasmaProperty concurrencyTimestampProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.optimistic, ConcurrentDataFlavor.time);
    if (concurrencyTimestampProperty == null) {
      // if (log.isDebugEnabled())
      // log.debug("could not find optimistic concurrency timestamp property for type, "
      // + type.getURI() + "#"
      // + type.getName());
    } else {
      Date dateSnapshot = new Date(this.snapshotMap.getSnapshotDate().getTime());
      Object snapshot = DataConverter.INSTANCE.convert(concurrencyTimestampProperty.getType(),
          dateSnapshot);
      if (!concurrencyTimestampProperty.isReadOnly())
        dataObject.set(concurrencyTimestampProperty, snapshot);
      else
        ((CoreDataObject) dataObject).setValue(concurrencyTimestampProperty.getName(), snapshot);
    }
  }

  // FIXME: need CG configuration related to enabling and handling concurrency
  // checks
  protected void updateOptimistic(PlasmaDataObject dataObject, long sequence, PlasmaType type,
      RowWriter rowContext) throws IOException {
    PlasmaProperty concurrencyUserProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.optimistic, ConcurrentDataFlavor.user);
    if (concurrencyUserProperty == null) {
      // if (log.isDebugEnabled())
      // log.debug("could not find optimistic concurrency (username) property for type, "
      // + type.getURI() + "#"
      // + type.getName());
    } else {
      byte[] bytes = HBaseDataConverter.INSTANCE.toBytes(concurrencyUserProperty, username);
      rowContext.writeRowData(dataObject, sequence, concurrencyUserProperty, bytes);
    }

    PlasmaProperty concurrencyTimestampProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.optimistic, ConcurrentDataFlavor.time);
    if (concurrencyTimestampProperty == null) {
      // if (log.isDebugEnabled())
      // log.debug("could not find optimistic concurrency timestamp property for type, "
      // + type.getURI() + "#"
      // + type.getName());
    } else {
      Date dateSnapshot = new Date(this.snapshotMap.getSnapshotDate().getTime());
      Object snapshot = DataConverter.INSTANCE.convert(concurrencyTimestampProperty.getType(),
          dateSnapshot);
      byte[] bytes = HBaseDataConverter.INSTANCE.toBytes(concurrencyTimestampProperty, snapshot);
      rowContext.writeRowData(dataObject, sequence, concurrencyTimestampProperty, bytes);
    }
  }

  protected void validateModifications(DataGraph dataGraph, PlasmaDataObject dataObject,
      RowWriter rowWriter) throws IllegalAccessException {
    PlasmaType type = (PlasmaType) dataObject.getType();
    if (log.isDebugEnabled())
      log.debug("updating " + type.getName() + " '"
          + ((PlasmaDataObject) dataObject).getUUIDAsString() + "'");
    PlasmaNode dataNode = (PlasmaNode) dataObject;

    PlasmaType rootType = (PlasmaType) rowWriter.getRootDataObject().getType();
    DataGraphMapping dataGraphConfig = StoreMapping.getInstance().getDataGraph(
        rootType.getQualifiedName());

    List<Property> properties = type.getProperties();
    for (Property p : properties) {
      PlasmaProperty property = (PlasmaProperty) p;

      Object oldValue = dataGraph.getChangeSummary().getOldValue(dataObject, property);
      if (oldValue == null)
        continue; // it's not been modified

      if (property.isReadOnly())
        throw new IllegalAccessException("attempt to modify read-only property, " + type.getURI()
            + "#" + type.getName() + "." + property.getName());

      UserDefinedRowKeyFieldMapping userDefinedField = dataGraphConfig
          .findUserDefinedRowKeyField(property);
      if (userDefinedField != null) {
        throw new IllegalAccessException("attempt to modify row-key property, " + type.getURI()
            + "#" + type.getName() + "." + property.getName()
            + " - this property is configured as a row-key field for table '"
            + dataGraphConfig.getTable().getName() + "'");
      }
      // FIXME: what if an entire entity is deleted which is part
      // of the row key. Detect this. Or added for that matter.
    }
  }

  protected void checkConcurrency(DataGraph dataGraph, PlasmaDataObject dataObject) {
    PlasmaType type = (PlasmaType) dataObject.getType();

    if (dataGraph.getChangeSummary().isCreated(dataObject)) {
      this.setOrigination(dataObject, type);
    } else if (dataGraph.getChangeSummary().isModified(dataObject)) {
      Timestamp snapshotDate = (Timestamp) ((CoreDataObject) dataObject)
          .getValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP);
      if (snapshotDate == null)
        throw new RequiredPropertyException("instance property '"
            + CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP + "' is required to update entity "
            + dataObject);
      // FIXME: check optimistic/pessimistic concurrency
      this.setOptimistic(dataObject, type, snapshotDate);
    } else if (dataGraph.getChangeSummary().isDeleted(dataObject)) {
      // FIXME: check optimistic/pessimistic concurrency
    }
  }

  // FIXME: need CG configuration related to enabling and handling concurrency
  // checks
  protected void checkOptimistic(PlasmaDataObject dataObject, PlasmaType type,
      Timestamp snapshotDate) {
    PlasmaProperty concurrencyUserProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.optimistic, ConcurrentDataFlavor.user);
    if (concurrencyUserProperty == null) {
      // if (log.isDebugEnabled())
      // log.debug("could not find optimistic concurrency (username) property for type, "
      // + type.getURI() + "#"
      // + type.getName());
    }

    PlasmaProperty concurrencyTimestampProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.optimistic, ConcurrentDataFlavor.time);
    if (concurrencyTimestampProperty == null) {
      // if (log.isDebugEnabled())
      // log.debug("could not find optimistic concurrency timestamp property for type, "
      // + type.getURI() + "#"
      // + type.getName());
    }
  }

  // FIXME: need CG configuration related to enabling and handling concurrency
  // checks
  protected void checkLock(PlasmaDataObject dataObject, PlasmaType type, Timestamp snapshotDate) {
    PlasmaProperty lockingUserProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.pessimistic, ConcurrentDataFlavor.user);
    // if (lockingUserProperty == null)
    // if (log.isDebugEnabled())
    // log.debug("could not find locking user property for type, " +
    // type.getURI() + "#" + type.getName());

    PlasmaProperty lockingTimestampProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.pessimistic, ConcurrentDataFlavor.time);
    // if (lockingTimestampProperty == null)
    // if (log.isDebugEnabled())
    // log.debug(
    // "could not find locking timestamp property for type, " +
    // type.getURI() + "#" + type.getName());

  }

  protected String toString(Map<String, DataObject> map) {
    StringBuilder buf = new StringBuilder();
    if (map != null) {
      Iterator<String> iter = map.keySet().iterator();
      int i = 0;
      while (iter.hasNext()) {
        if (i > 0)
          buf.append(", ");
        buf.append(iter.next());
      }
    }
    return buf.toString();
  }

}
