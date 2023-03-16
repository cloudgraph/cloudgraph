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
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.rocksdb.io.DistributedWriter;
import org.cloudgraph.rocksdb.io.EdgeWriter;
import org.cloudgraph.rocksdb.io.RowWriter;
import org.cloudgraph.rocksdb.io.TableWriter;
import org.cloudgraph.rocksdb.service.RocksDBDataConverter;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaEdge;
import org.plasma.sdo.PlasmaNode;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.RequiredPropertyException;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.core.CoreNode;
import org.plasma.sdo.core.SnapshotMap;
import org.plasma.sdo.helper.DataConverter;
import org.plasma.sdo.profile.ConcurrencyType;
import org.plasma.sdo.profile.ConcurrentDataFlavor;

import commonj.sdo.DataGraph;
import commonj.sdo.Property;

public class Create extends DefaultMutation implements Mutation {
  private static Log log = LogFactory.getLog(Create.class);

  public Create(ServiceContext context, SnapshotMap snapshotMap, String username) {
    super(context, snapshotMap, username);
  }

  /**
   * For create operation initializes the entity sequence and populates the
   * entity metadata
   */
  @Override
  public void init(DataGraph dataGraph, PlasmaDataObject dataObject, RowWriter rowWriter)
      throws IllegalAccessException, IOException {
    PlasmaType type = (PlasmaType) dataObject.getType();
    CoreNode coreNode = ((CoreNode) dataObject);
    boolean typeBound = StoreMapping.getInstance().findTable(type.getQualifiedName(),
        this.context.getStoreMapping()) != null;

    long sequence = CloudGraphConstants.ROOT_SEQUENCE;
    // if were not creating a root in this or another graph
    if (!dataObject.equals(dataGraph.getRootObject()) && !typeBound) {
      if (rowWriter.containsSequence(dataObject)) {
        sequence = rowWriter.getSequence(dataObject);
      } else {
        sequence = rowWriter.newSequence(dataObject);
        rowWriter.addSequence(dataObject, sequence);
      }
    }
    coreNode.setValue(CloudGraphConstants.SEQUENCE, sequence);
    rowWriter.writeRowEntityMetaData(dataObject, sequence);
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
    this.setupOptimistic(dataGraph, dataObject, type, sequence, rowWriter);
    this.setupOrigination(dataGraph, dataObject, type, sequence, rowWriter);

  }

  /**
   * Iterates over every property in the data object and pulls its value, as for
   * 'create' operations, change summary does not register which properties are
   * changed, only that the object is created.
   */
  @Override
  public void collect(DataGraph dataGraph, PlasmaDataObject dataObject,
      DistributedWriter graphWriter, TableWriter tableWriter, RowWriter rowWriter)
      throws IOException, IllegalAccessException {
    PlasmaType type = (PlasmaType) dataObject.getType();
    PlasmaNode dataNode = (PlasmaNode) dataObject;
    CoreNode coreNode = (CoreNode) dataObject;

    // boolean typeBound =
    // StoreMapping.getInstance().findTable(type.getQualifiedName()) != null
    // long sequence = CloudGraphConstants.ROOT_SEQUENCE;
    // // if were not creating a root in this or another graph
    // if (!dataObject.equals(dataGraph.getRootObject()) && !typeBound) {
    // if (rowWriter.containsSequence(dataObject)) {
    // sequence = rowWriter.getSequence(dataObject);
    // } else {
    // sequence = rowWriter.newSequence(dataObject);
    // rowWriter.addSequence(dataObject, sequence);
    // }
    // }
    // coreNode.setValue(CloudGraphConstants.SEQUENCE, sequence);
    // rowWriter.writeRowEntityMetaData(dataObject, sequence);

    Long sequence = (Long) coreNode.getValue(CloudGraphConstants.SEQUENCE);
    if (sequence == null)
      throw new RequiredPropertyException("instance property '" + CloudGraphConstants.SEQUENCE
          + "' is required to update data object, " + dataObject);

    // this.updateOrigination(dataObject, sequence, type, rowWriter);
    // //FIXME: duplicate logic in GraphRotWriter
    // this.updateOptimistic(dataObject, sequence, type, rowWriter);

    // Set<EdgeWriter> edgeWriters = null;
    // For 'create' change summary has no changed property specifics
    // FIXME: use new DO value iterator
    List<Property> properties = type.getProperties();
    for (Property p : properties) {
      PlasmaProperty property = (PlasmaProperty) p;
      // if (property.isKey(KeyType.primary) &&
      // property.getType().isDataType())
      // continue; // processed above

      if (property.getConcurrent() != null)
        continue; // processed above in setup()

      Object dataValue = dataObject.get(property);
      if (dataValue == null)
        continue; // nothing to create

      byte[] valueBytes = null;
      if (!property.getType().isDataType()) {
        List<PlasmaEdge> edges = dataNode.getEdges(property);

        List<PlasmaEdge> ownedEdges = this.findOwnedEdges(dataNode, property, edges, graphWriter,
            rowWriter);
        if (ownedEdges.size() > 0) {
          // write local owned edges
          EdgeWriter edgeWriter = rowWriter.getEdgeWriter(dataObject, property, sequence);
          edgeWriter.addAll(dataNode, ownedEdges);
          edgeWriter.write();
        }
      } else {
        valueBytes = RocksDBDataConverter.INSTANCE.toBytes(property, dataValue);
        rowWriter.writeRowData(dataObject, sequence, property, valueBytes);
      }
    }
  }

}
