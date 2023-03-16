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
package org.cloudgraph.rocksdb.mutation;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.rocksdb.io.DistributedWriter;
import org.cloudgraph.rocksdb.io.EdgeWriter;
import org.cloudgraph.rocksdb.io.RowWriter;
import org.cloudgraph.rocksdb.io.TableWriter;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.RequiredPropertyException;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.core.CoreNode;
import org.plasma.sdo.core.SnapshotMap;
import org.plasma.sdo.profile.ConcurrencyType;
import org.plasma.sdo.profile.ConcurrentDataFlavor;

import commonj.sdo.DataGraph;
import commonj.sdo.Property;

public class Delete extends DefaultMutation implements Mutation {
  private static Log log = LogFactory.getLog(Delete.class);

  public Delete(ServiceContext context, SnapshotMap snapshotMap, String username) {
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
    // unless we're deleting entire row
    if (!rowWriter.isRootDeleted()) {
      this.setupOptimistic(dataGraph, dataObject, type, sequence, rowWriter);
      this.setupOrigination(dataGraph, dataObject, type, sequence, rowWriter);
    }
  }

  @Override
  public void collect(DataGraph dataGraph, PlasmaDataObject dataObject,
      DistributedWriter graphWriter, TableWriter tableWriter, RowWriter rowWriter)
      throws IOException, IllegalAccessException {
    PlasmaType type = (PlasmaType) dataObject.getType();

    CoreNode coreNode = ((CoreNode) dataObject);
    // FIXME: get rid of cast - define instance properties in 'base type'
    Timestamp snapshotDate = (Timestamp) coreNode
        .getValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP);
    if (snapshotDate == null)
      throw new RequiredPropertyException("instance property '"
          + CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP
          + "' is required to update data object, " + dataObject);
    if (log.isDebugEnabled())
      log.debug(dataObject + " timestamp: " + String.valueOf(snapshotDate));

    Long sequence = (Long) coreNode.getValue(CloudGraphConstants.SEQUENCE);
    if (sequence == null)
      throw new RequiredPropertyException("instance property '" + CloudGraphConstants.SEQUENCE
          + "' is required to delete data object, " + dataObject);
    if (log.isDebugEnabled())
      log.debug(dataObject + " (seq: " + sequence + ")");

    // If no tombstones, and if the root of the row is deleted if this
    // object is the root, delete it, else if
    // the given data object is part of the row, just return.
    // No need to delete its individual properties, as the row is going away
    // anyway.
    if (!tableWriter.getTableConfig().tombstoneRows()) {
      if (rowWriter.isRootDeleted()) {
        if (rowWriter.getRootDataObject().equals(dataObject)) {
          rowWriter.deleteRow();
          if (log.isDebugEnabled())
            log.debug("deleting root, " + dataObject);
          return;
        } else if (rowWriter.contains(dataObject)) {
          return;
        }
      }
    }

    // For tombstone rows, we want to delete app columns (and leave a
    // tombstone column
    // which is not done here).
    // So delete all columns associated with the entity
    rowWriter.deleteRowEntityMetaData(dataObject, sequence);

    List<Property> properties = type.getProperties();
    for (Property p : properties) {
      PlasmaProperty property = (PlasmaProperty) p;
      if (property.getType().isDataType()) {
        rowWriter.deleteRowData(dataObject, sequence, property);
        // this.deleteDataCell(rowWriter, dataObject, sequence,
        // property);
      } else {
        EdgeWriter edgeWriter = rowWriter.getEdgeWriter(dataObject, property, sequence);
        edgeWriter.delete();
      }
    }
  }

}
