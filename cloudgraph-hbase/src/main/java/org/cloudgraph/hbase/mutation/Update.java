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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.hbase.io.DistributedWriter;
import org.cloudgraph.hbase.io.EdgeWriter;
import org.cloudgraph.hbase.io.RowWriter;
import org.cloudgraph.hbase.io.TableWriter;
import org.cloudgraph.hbase.service.HBaseDataConverter;
import org.cloudgraph.hbase.service.ServiceContext;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.sdo.Concurrent;
import org.plasma.sdo.DataType;
import org.plasma.sdo.Increment;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaEdge;
import org.plasma.sdo.PlasmaNode;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.RequiredPropertyException;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.core.CoreNode;
import org.plasma.sdo.core.SnapshotMap;
import org.plasma.sdo.helper.DataConverter;
import org.plasma.sdo.profile.ConcurrentDataFlavor;

import commonj.sdo.ChangeSummary.Setting;
import commonj.sdo.DataGraph;

public class Update extends DefaultMutation implements Mutation {
  private static Log log = LogFactory.getLog(Update.class);

  public Update(ServiceContext context, SnapshotMap snapshotMap, String username) {
    super(context, snapshotMap, username);
  }

  @Override
  public void init(DataGraph dataGraph, PlasmaDataObject dataObject, RowWriter rowWriter)
      throws IllegalAccessException, IOException {
    // noop
  }

  @Override
  public void validate(DataGraph dataGraph, PlasmaDataObject dataObject, RowWriter rowWriter)
      throws IllegalAccessException {
    this.validateModifications(dataGraph, dataObject, rowWriter);
  }

  @Override
  public void setup(DataGraph dataGraph, PlasmaDataObject dataObject, RowWriter rowWriter)
      throws IllegalAccessException, IOException {
    CoreNode coreNode = (CoreNode) dataObject;
    PlasmaType type = (PlasmaType) dataObject.getType();
    Long sequence = (Long) coreNode.getValue(CloudGraphConstants.SEQUENCE);
    if (sequence == null)
      throw new RequiredPropertyException("instance property '" + CloudGraphConstants.SEQUENCE
          + "' is required to update data object, " + dataObject);
    this.setupOptimistic(dataGraph, dataObject, type, sequence, rowWriter);
    this.setupOrigination(dataGraph, dataObject, type, sequence, rowWriter);
  }

  @Override
  public void collect(DataGraph dataGraph, PlasmaDataObject dataObject,
      DistributedWriter graphWriter, TableWriter tableWriter, RowWriter rowWriter)
      throws IllegalAccessException, IOException {
    PlasmaType type = (PlasmaType) dataObject.getType();
    CoreNode dataNode = (CoreNode) dataObject;
    // FIXME: get rid of cast - define instance properties in 'base type'
    Timestamp snapshotDate = (Timestamp) dataNode
        .getValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP);
    if (snapshotDate == null)
      throw new RequiredPropertyException("instance property '"
          + CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP
          + "' is required to update data object, " + dataObject);
    if (log.isDebugEnabled())
      log.debug(dataObject + " timestamp: " + String.valueOf(snapshotDate));

    Long sequence = (Long) dataNode.getValue(CloudGraphConstants.SEQUENCE);
    if (sequence == null)
      throw new RequiredPropertyException("instance property '" + CloudGraphConstants.SEQUENCE
          + "' is required to update data object, " + dataObject);

    if (log.isDebugEnabled())
      log.debug(dataObject + " (seq: " + sequence + ")");

    List<Setting> settingList = dataGraph.getChangeSummary().getOldValues(dataObject);
    HashSet<PlasmaProperty> properties = this.collectProperties(settingList);
    Iterator<PlasmaProperty> iter = properties.iterator();
    while (iter.hasNext()) {
      PlasmaProperty property = iter.next();

      if (property.getConcurrent() != null)
        return; // processed above

      if (property.isReadOnly())
        throw new IllegalAccessException("attempt to modify read-only property, " + property);

      Object dataValue = dataObject.get(property);
      if (dataValue != null)
        if (log.isDebugEnabled())
          log.debug("updating " + property.toString());
        else if (log.isDebugEnabled())
          log.debug("removing " + property.toString());

      if (!property.getType().isDataType()) {
        SettingCollector<PlasmaDataObject> settingCollector = new SettingCollector<>();
        HashSet<PlasmaDataObject> oldSettings = settingCollector.collect(property, settingList);
        HashSet<PlasmaDataObject> oldValues = new HashSet<>(oldSettings.size());
        for (PlasmaDataObject oldSettingObject : oldSettings) {
          if (!oldSettingObject.getDataGraph().getChangeSummary().isCreated(oldSettingObject))
            oldValues.add(oldSettingObject);
        }

        EdgeWriter edgeWriter = rowWriter.getEdgeWriter(dataObject, property, sequence);
        if (!property.isMany()) {
          this.collectSingular(edgeWriter, dataObject, oldValues, property, dataValue);
        } else {
          this.collectMulti(edgeWriter, dataObject, oldValues, property, dataValue);
        }
        edgeWriter.write();
      } else {
        Increment increment = property.getIncrement();
        if (dataValue != null) {
          if (increment == null) {
            byte[] valueBytes = HBaseDataConverter.INSTANCE.toBytes(property, dataValue);
            rowWriter.writeRowData(dataObject, sequence, property, valueBytes);
          } else { // increment
            if (type.isConcurrent())
              throw new GraphServiceException("increment property, " + property
                  + ", found on concurrent type, " + type
                  + " - increment properties cannot coexist within a concurrent type");
            DataType dataType = DataType.valueOf(property.getType().getName());
            if (increment != null) { // user can increment/decrement by whatever
                                     // value
              if (dataType.ordinal() != DataType.Long.ordinal())
                throw new GraphServiceException("property, " + property + ", must be datatype "
                    + DataType.Long + " to support increment operations");
              long longDataValue = DataConverter.INSTANCE.toLong(property.getType(), dataValue);
              rowWriter.incrementRowData(dataObject, sequence, property, longDataValue);
            }
          }

        } else {
          rowWriter.deleteRowData(dataObject, sequence, property);
        }
      }
    }
  }

  /**
   * Returns the unique set of properties for the given setting list.
   * 
   * @param settings
   *          the settings
   * @return the unique properties
   */
  private HashSet<PlasmaProperty> collectProperties(List<Setting> settings) {
    HashSet<PlasmaProperty> result = new HashSet<>();
    for (Setting setting : settings) {
      PlasmaProperty property = (PlasmaProperty) setting.getProperty();
      result.add(property);
    }
    return result;
  }

  private void collectSingular(EdgeWriter edgeWriter, PlasmaDataObject dataObject,
      HashSet<PlasmaDataObject> oldValues, PlasmaProperty property, Object value)
      throws IOException {

    PlasmaNode dataNode = (PlasmaNode) dataObject;
    List<PlasmaEdge> edges = dataNode.getEdges(property);

    PlasmaDataObject oldDataObject = null;
    if (oldValues.size() > 0) {
      if (oldValues.size() == 1) {
        oldDataObject = (PlasmaDataObject) oldValues.iterator().next();
      } else
        throw new GraphServiceException("unexpected List as old value for property, "
            + property.toString());
    }
    // if has a new value
    PlasmaDataObject newDataObject = null;
    if (value != null) {
      PlasmaEdge edge = edges.get(0);

      newDataObject = edge.getOpposite(dataNode).getDataObject();
      if (oldDataObject != null)
        edgeWriter.replace(oldDataObject, newDataObject);
      else
        // add the new one
        edgeWriter.add(newDataObject);
    } else {
      if (oldDataObject != null)
        edgeWriter.remove(oldDataObject);
    }
  }

  private void collectMulti(EdgeWriter edgeWriter, PlasmaDataObject dataObject,
      HashSet<PlasmaDataObject> oldValues, PlasmaProperty property, Object value)
      throws IOException {

    PlasmaNode dataNode = (PlasmaNode) dataObject;
    List<PlasmaEdge> updatedEdges = dataNode.getEdges(property);
    if (log.isDebugEnabled())
      log.debug("merging " + String.valueOf(oldValues.size()) + " old and "
          + String.valueOf(updatedEdges.size()) + " current values");
    edgeWriter.merge(dataNode, oldValues, updatedEdges);
  }

}
