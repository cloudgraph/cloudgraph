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
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaEdge;
import org.plasma.sdo.PlasmaNode;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.RequiredPropertyException;
import org.plasma.sdo.access.provider.common.PropertyPair;
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
  protected StoreMappingContext mappingContext;

  public DefaultMutation(ServiceContext context, SnapshotMap snapshotMap, String username,
      StoreMappingContext mappingContext) {
    super();
    this.snapshotMap = snapshotMap;
    this.username = username;
    this.context = context;
    this.mappingContext = mappingContext;
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
          oppositeType.getQualifiedName(), this.mappingContext) != null;
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

  protected void setupOptimistic(DataGraph dataGraph, PlasmaDataObject dataObject, PlasmaType type,
      long sequence, RowWriter rowWriter) throws IOException {
    PlasmaProperty concurrencyUserProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.optimistic, ConcurrentDataFlavor.user);
    if (concurrencyUserProperty != null) {
      byte[] bytes = HBaseDataConverter.INSTANCE.toBytes(concurrencyUserProperty, username);
      rowWriter.writeRowData(dataObject, sequence, concurrencyUserProperty, bytes);
      this.snapshotMap.put(dataObject.getUUID(),
          new PropertyPair(concurrencyUserProperty, username));
    }

    PlasmaProperty concurrencyTimestampProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.optimistic, ConcurrentDataFlavor.time);
    if (concurrencyTimestampProperty != null) {
      Date dateSnapshot = new Date(this.snapshotMap.getSnapshotDate().getTime());
      Object snapshot = DataConverter.INSTANCE.convert(concurrencyTimestampProperty.getType(),
          dateSnapshot);
      byte[] bytes = HBaseDataConverter.INSTANCE.toBytes(concurrencyTimestampProperty, snapshot);
      rowWriter.writeRowData(dataObject, sequence, concurrencyTimestampProperty, bytes);
      this.snapshotMap.put(dataObject.getUUID(), new PropertyPair(concurrencyTimestampProperty,
          dateSnapshot));
    }

    PlasmaProperty concurrencyVersionProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.optimistic, ConcurrentDataFlavor.version);
    if (concurrencyVersionProperty != null) {
      long longVersion = 0;
      if (dataObject.isSet(concurrencyVersionProperty))
        longVersion = dataObject.getLong(concurrencyVersionProperty);
      longVersion++;
      Object version = DataConverter.INSTANCE.convert(concurrencyVersionProperty.getType(),
          longVersion);
      byte[] bytes = HBaseDataConverter.INSTANCE.toBytes(concurrencyVersionProperty, version);
      // write the incremented version into both the row mutation
      // and the snapshot map, so on concurrency failure, neither of these
      // will succeed
      rowWriter.writeRowData(dataObject, sequence, concurrencyVersionProperty, bytes);
      this.snapshotMap.put(dataObject.getUUID(), new PropertyPair(concurrencyVersionProperty,
          new Long(longVersion)));
    }

  }

  protected void setupOrigination(DataGraph dataGraph, PlasmaDataObject dataObject,
      PlasmaType type, long sequence, RowWriter rowWriter) throws IOException {
    PlasmaProperty originationUserProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.origination, ConcurrentDataFlavor.user);
    if (originationUserProperty != null) {
      byte[] bytes = HBaseDataConverter.INSTANCE.toBytes(originationUserProperty, username);
      rowWriter.writeRowData(dataObject, sequence, originationUserProperty, bytes);
      this.snapshotMap.put(dataObject.getUUID(),
          new PropertyPair(originationUserProperty, username));
    }

    PlasmaProperty originationTimestampProperty = (PlasmaProperty) type.findProperty(
        ConcurrencyType.origination, ConcurrentDataFlavor.time);
    if (originationTimestampProperty != null) {
      Date dateSnapshot = new Date(this.snapshotMap.getSnapshotDate().getTime());
      Object snapshot = DataConverter.INSTANCE.convert(originationTimestampProperty.getType(),
          dateSnapshot);
      byte[] bytes = HBaseDataConverter.INSTANCE.toBytes(originationTimestampProperty, snapshot);
      rowWriter.writeRowData(dataObject, sequence, originationTimestampProperty, bytes);
      this.snapshotMap.put(dataObject.getUUID(), new PropertyPair(originationTimestampProperty,
          dateSnapshot));
    }
  }

  protected void validateModifications(DataGraph dataGraph, PlasmaDataObject dataObject,
      RowWriter rowWriter) throws IllegalAccessException {
    PlasmaType type = (PlasmaType) dataObject.getType();
    if (log.isDebugEnabled())
      log.debug("updating " + type.getName() + " '"
          + ((PlasmaDataObject) dataObject).getUUIDAsString() + "'");
    PlasmaType rootType = (PlasmaType) rowWriter.getRootDataObject().getType();
    DataGraphMapping dataGraphConfig = StoreMapping.getInstance().getDataGraph(
        rootType.getQualifiedName(), this.mappingContext);

    List<Property> properties = type.getProperties();
    for (Property p : properties) {
      PlasmaProperty property = (PlasmaProperty) p;

      Object oldValue = dataGraph.getChangeSummary().getOldValue(dataObject, property);
      if (oldValue == null)
        continue; // it's not been modified

      if (property.isReadOnly())
        throw new IllegalAccessException("attempt to modify read-only property, " + type.getURI()
            + "#" + type.getName() + "." + property.getName());

      DataRowKeyFieldMapping userDefinedField = dataGraphConfig
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
