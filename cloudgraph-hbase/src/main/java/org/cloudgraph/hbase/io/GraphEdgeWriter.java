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
package org.cloudgraph.hbase.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaEdge;
import org.plasma.sdo.PlasmaNode;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.RequiredPropertyException;
import org.plasma.sdo.core.CoreNode;

public class GraphEdgeWriter extends DefaultEdgeOperation implements EdgeWriter {
  private static Log log = LogFactory.getLog(GraphEdgeWriter.class);

  private RowWriter rowWriter;
  private PlasmaDataObject dataObject;

  private Long typeSequenceNum;

  GraphEdgeWriter(PlasmaDataObject source, PlasmaProperty sourceProp, TableMapping tableConfig,
      DataGraphMapping graphConfig, RowWriter rowWriter) throws IOException {
    super((PlasmaType) source.getType(), sourceProp, tableConfig, graphConfig);
    this.dataObject = source;
    this.rowWriter = rowWriter;

    this.baseTypeQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.sourceProp, EdgeMetaKey.BASETYPE);
    this.subTypeQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.sourceProp, EdgeMetaKey.SUBTYPE);
    this.pathQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.sourceProp, EdgeMetaKey.PATH);
    this.sequencesQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.sourceProp, EdgeMetaKey.SEQUENCES);
    // this.tableQual =
    // this.rowWriter.getColumnKeyFactory().createColumnKey(
    // this.sourceType, this.sourceProp, EdgeMetaField._TBL_);
    this.keysQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.sourceProp, EdgeMetaKey.ROWKEYS);
    this.countQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.sourceProp, EdgeMetaKey.COUNT);
  }

  GraphEdgeWriter(PlasmaDataObject source, PlasmaProperty sourceProp, Long typeSequenceNum,
      TableMapping tableConfig, DataGraphMapping graphConfig, RowWriter rowWriter)
      throws IOException {

    super((PlasmaType) source.getType(), sourceProp, tableConfig, graphConfig);
    this.dataObject = source;
    this.typeSequenceNum = typeSequenceNum;
    this.rowWriter = rowWriter;

    this.baseTypeQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.typeSequenceNum, this.sourceProp, EdgeMetaKey.BASETYPE);
    this.subTypeQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.typeSequenceNum, this.sourceProp, EdgeMetaKey.SUBTYPE);
    this.pathQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.typeSequenceNum, this.sourceProp, EdgeMetaKey.PATH);
    this.sequencesQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.typeSequenceNum, this.sourceProp, EdgeMetaKey.SEQUENCES);
    // this.tableQual =
    // this.rowWriter.getColumnKeyFactory().createColumnKey(
    // this.sourceType, this.typeSequenceNum, this.sourceProp,
    // EdgeMetaField._TBL_);
    this.keysQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.typeSequenceNum, this.sourceProp, EdgeMetaKey.ROWKEYS);
    this.countQual = this.rowWriter.getColumnKeyFactory().createColumnKey(this.sourceType,
        this.typeSequenceNum, this.sourceProp, EdgeMetaKey.COUNT);
  }

  /**
   * Set up metadata where we know it can't exist yet i.e. for a created data
   * object where no fields/edges yet exist. throws IllegalStateException if the
   * underlying data object is not created in the change summary
   */
  private void initMetadata() {
    if (!this.dataObject.getDataGraph().getChangeSummary().isCreated(this.dataObject))
      throw new IllegalStateException("expected created data object, " + dataObject);
    this.collectionBaseType = (PlasmaType) sourceProp.getType();
    // Note: can be updated later is subtype differs
    this.collectionPath = "/";
  }

  /**
   * Set up metadata where we know it must exist yet i.e. for a modified data
   * object where ???. throws IllegalStateException if the underlying data
   * object is not modified in the change summary throws IllegalStateException
   * if no qualified metadata columns are found for for the row.
   * 
   * @throws IOException
   */
  private void fetchMetadata() throws IOException {
    if (!this.dataObject.getDataGraph().getChangeSummary().isModified(this.dataObject))
      throw new IllegalStateException("expected modified data object, " + dataObject);
    Get existing = new Get(rowWriter.getRowKey());

    existing.addColumn(family, baseTypeQual);
    existing.addColumn(family, subTypeQual);
    existing.addColumn(family, pathQual);
    existing.addColumn(family, sequencesQual);
    // existing.addColumn(family, tableQual);
    existing.addColumn(family, keysQual);
    existing.addColumn(family, countQual);

    Result result = this.rowWriter.getTableWriter().getTable().get(existing);
    if (result.getRow() == null) {
      throw new IllegalStateException("fetch failed for collection metadata properties "
          + "for key: '" + Bytes.toString(existing.getRow()) + "' on object, " + this.dataObject);
    }
    collectMetadata(result);
  }

  /**
   * Set up metadata where we know it may exist yet i.e. for a modified data
   * object where ???. throws IllegalStateException if the underlying data
   * object is not modified in the change summary
   * 
   * @throws IOException
   */
  private void findMetadata() throws IOException {
    if (!this.dataObject.getDataGraph().getChangeSummary().isModified(this.dataObject))
      throw new IllegalStateException("expected modified data object, " + dataObject);
    Get existing = new Get(rowWriter.getRowKey());

    existing.addColumn(family, baseTypeQual);
    existing.addColumn(family, subTypeQual);
    existing.addColumn(family, pathQual);
    existing.addColumn(family, sequencesQual);
    // existing.addColumn(family, tableQual);
    existing.addColumn(family, keysQual);
    existing.addColumn(family, countQual);

    Result result = this.rowWriter.getTableWriter().getTable().get(existing);
    if (result.getRow() == null) {
      this.collectionBaseType = (PlasmaType) sourceProp.getType();
      this.collectionPath = "/";
    } else {
      collectMetadata(result);
    }
  }

  private void collectMetadata(Result result) {
    if (log.isDebugEnabled()) {
      log.debug("row: " + Bytes.toString(result.getRow()));
      List<KeyValue> cells = result.list();
      if (cells != null)
        for (KeyValue keyValue : cells) {
          log.debug("\tkey: " + new String(keyValue.getQualifier()) + "\tvalue: "
              + new String(keyValue.getValue()));
        }
    }

    byte[] typeBytes = findValue(family, baseTypeQual, result);
    if (typeBytes == null || typeBytes.length == 0)
      throw new IllegalStateException("fetch failed for mandatory collection metadata property, "
          + EdgeMetaKey.BASETYPE + " (" + Bytes.toString(baseTypeQual) + ") for key: '"
          + Bytes.toString(result.getRow()) + "'");
    this.decodeCollectionBaseType(typeBytes);

    byte[] subTypeBytes = findValue(family, subTypeQual, result);
    if (subTypeBytes != null)
      this.decodeDefaultSubType(subTypeBytes);

    byte[] pathBytes = findValue(family, pathQual, result);
    if (pathBytes != null)
      this.collectionPath = new String(pathBytes, this.charset);
    else
      this.collectionPath = "/";

    byte[] countBytes = findValue(family, countQual, result);
    if (countBytes == null || countBytes.length == 0)
      throw new IllegalStateException("fetch failed for mandatory collection metadata property, "
          + EdgeMetaKey.COUNT + " (" + Bytes.toString(countQual) + ") for key: '"
          + Bytes.toString(result.getRow()) + "'");
    long count = Long.valueOf(new String(countBytes, charset));

    byte[] sequenceBytes = findValue(family, sequencesQual, result);
    if (sequenceBytes != null) {
      this.decodeSequences(sequenceBytes);
      if (count != this.sequences.size())
        throw new IllegalStateException("invalid value " + count + " for qualifier "
            + EdgeMetaKey.COUNT + " (" + Bytes.toString(countQual) + ") for key: '"
            + Bytes.toString(result.getRow()) + "' - expected " + this.sequences.size());
    }
    byte[] rowKeyBytes = findValue(family, keysQual, result);
    if (rowKeyBytes != null) {
      this.decodeRowKeys(rowKeyBytes);
      if (count != this.rowKeys.size())
        throw new IllegalStateException("invalid value " + count + " for qualifier "
            + EdgeMetaKey.COUNT + " (" + Bytes.toString(countQual) + ") for key: '"
            + Bytes.toString(result.getRow()) + "' - expected " + this.rowKeys.size());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.EdgeWriter#getRowWriter()
   */
  @Override
  public RowWriter getRowWriter() {
    return rowWriter;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.hbase.io.EdgeWriter#replace(org.plasma.sdo.PlasmaDataObject
   * , org.plasma.sdo.PlasmaDataObject)
   */
  @Override
  public void replace(PlasmaDataObject oldValue, PlasmaDataObject newValue) throws IOException {
    if (log.isDebugEnabled())
      log.debug("replacing old value " + oldValue + " with new value " + newValue + " for "
          + this.dataObject);
    remove(oldValue);
    add(newValue);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.hbase.io.EdgeWriter#remove(org.plasma.sdo.PlasmaDataObject )
   */
  @Override
  public void remove(PlasmaDataObject oldValue) throws IOException {
    if (oldValue == null)
      throw new IllegalArgumentException("expected arg, oldValue");
    if (this.collectionBaseType == null)
      fetchMetadata();
    if (!isExternal()) {
      Long oldSequence = null;
      if (oldValue.getDataGraph().getChangeSummary().isCreated(oldValue))
        oldSequence = getOrCreateSequence(oldValue);
      else
        oldSequence = getSequence(oldValue);
      if (log.isDebugEnabled())
        log.debug("removing old value " + oldValue + " (seq: " + oldSequence + ") from "
            + this.dataObject);
      if (!this.sequences.remove(oldSequence))
        throw new IllegalStateException("could not remove sequence(" + oldSequence
            + ") for old value, " + oldValue);
      if (this.sequences.size() == 0)
        this.collectionDefaultSubType = null; // so can reset w/new type
      // potentially
    } else {
      CoreNode oldCoreNode = (CoreNode) oldValue;
      byte[] oldRowKey = (byte[]) oldCoreNode.getValueObject().get(CloudGraphConstants.ROW_KEY);
      if (oldRowKey == null) {
        // FIXME: something more specific and informative
        throw new RequiredPropertyException("instance property '" + CloudGraphConstants.ROW_KEY
            + "' is required to remove an existing data object, " + oldValue);
      }

      if (log.isDebugEnabled())
        log.debug("removing old value " + oldValue + " (key: '" + Bytes.toString(oldRowKey)
            + "') from " + this.dataObject);
      if (!this.rowKeys.remove(new KeyBytes(oldRowKey)))
        throw new IllegalStateException("could not remove row key for old value, " + oldValue
            + " (key: '" + Bytes.toString(oldRowKey) + "')");
      if (this.rowKeys.size() == 0)
        this.collectionDefaultSubType = null; // so can reset w/new type
      // potentially
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.hbase.io.EdgeWriter#add(org.plasma.sdo.PlasmaDataObject)
   */
  @Override
  public void add(PlasmaDataObject newValue) throws IOException {
    if (newValue == null)
      throw new IllegalArgumentException("expected arg, newValue");
    if (this.collectionBaseType == null)
      findMetadata();
    PlasmaType newValueType = (PlasmaType) newValue.getType();
    // init subtype if necessary
    if (this.collectionDefaultSubType == null) {
      if (!newValueType.equals(this.collectionBaseType)) {
        this.collectionDefaultSubType = newValueType;
      }
    }
    this.checkNewEdgeType(newValueType);

    if (!isExternal()) {
      Long seq = null;
      if (newValue.getDataGraph().getChangeSummary().isCreated(newValue)) {
        seq = getOrCreateSequence(newValue);
      } else {
        seq = getSequence(newValue);
      }
      if (log.isDebugEnabled())
        log.debug("adding new value " + newValue + " (seq: " + seq + ") to " + this.dataObject);
      if (this.sequences == null)
        this.sequences = new ArrayList<Long>();
      if (this.sequences.contains(seq))
        throw new IllegalStateException("duplicate sequence found for opposite, " + newValue);
      this.sequences.add(seq);
      if (newValue.getDataGraph().getChangeSummary().isCreated(newValue))
        rowWriter.writeRowEntityMetaData(dataObject, seq);
    } else {
      CoreNode newCoreNode = (CoreNode) newValue;
      byte[] newRowKey = (byte[]) newCoreNode.getValueObject().get(CloudGraphConstants.ROW_KEY);
      if (newRowKey == null && !newValue.getDataGraph().getChangeSummary().isCreated(newValue)) {
        // FIXME: something more specific and informative
        throw new RequiredPropertyException("instance property '" + CloudGraphConstants.ROW_KEY
            + "' is required to add an existing data object, " + newValue);
      }
      if (newRowKey == null) {
        DistributedWriter distributedWriter = this.rowWriter.getTableWriter()
            .getDistributedWriter();
        RowWriter oppositeRowWriter = distributedWriter.findRowWriter(newValue);
        if (oppositeRowWriter == null)
          oppositeRowWriter = distributedWriter.createRowWriter(newValue);
        newRowKey = oppositeRowWriter.getRowKey();
      }
      if (log.isDebugEnabled())
        log.debug("adding new value " + newValue + " (key: '" + Bytes.toString(newRowKey)
            + "') to " + this.dataObject);
      this.addRowKey(newRowKey);
    }
  }

  private void checkNewEdgeType(PlasmaType newValueType) {
    if (this.collectionDefaultSubType != null) {
      if (!newValueType.equals(this.collectionDefaultSubType))
        throw new EdgeTypeMismatchException("given object type, " + newValueType
            + ", does not match edge/collection subtype, " + this.collectionDefaultSubType);
    } else {
      if (!newValueType.equals(this.collectionBaseType))
        throw new EdgeTypeMismatchException("given object type, " + newValueType
            + ", does not match edge/collection type, " + this.collectionBaseType);

    }
  }

  // create operation specific?
  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.EdgeWriter#addAll(org.plasma.sdo.PlasmaNode,
   * java.util.List)
   */
  @Override
  public void addAll(PlasmaNode dataNode, List<PlasmaEdge> edges) throws IOException {
    if (this.collectionBaseType == null)
      initMetadata();

    DistributedWriter distributedWriter = null;

    for (PlasmaEdge edge : edges) {
      PlasmaDataObject opposite = edge.getOpposite(dataNode).getDataObject();
      PlasmaType oppositeType = (PlasmaType) opposite.getType();

      // init subtype if necessary
      if (this.collectionDefaultSubType == null) {
        if (!oppositeType.equals(this.collectionBaseType)) {
          this.collectionDefaultSubType = oppositeType;
        }
      }
      this.checkNewEdgeType(oppositeType);

      if (!isExternal()) {
        if (this.sequences == null)
          this.sequences = new ArrayList<Long>(edges.size());
        Long seq = null;
        if (opposite.getDataGraph().getChangeSummary().isCreated(opposite))
          seq = getOrCreateSequence(opposite);
        else
          seq = getSequence(opposite);
        if (this.sequences.contains(seq))
          throw new IllegalStateException("duplicate sequence found for opposite, " + opposite);
        this.sequences.add(seq);
      } else {
        CoreNode oppositeCoreNode = (CoreNode) opposite;
        byte[] rowKey = (byte[]) oppositeCoreNode.getValueObject().get(CloudGraphConstants.ROW_KEY);
        if (rowKey == null && !opposite.getDataGraph().getChangeSummary().isCreated(opposite)) {
          // FIXME: something more specific and informative
          throw new RequiredPropertyException("instance property '" + CloudGraphConstants.ROW_KEY
              + "' is required to add an existing data object, " + opposite);
        }
        if (rowKey == null) {
          if (distributedWriter == null)
            distributedWriter = this.rowWriter.getTableWriter().getDistributedWriter();
          RowWriter oppositeRowWriter = distributedWriter.findRowWriter(opposite);
          if (oppositeRowWriter == null) {
            oppositeRowWriter = distributedWriter.createRowWriter(opposite);
          }
          rowKey = oppositeRowWriter.getRowKey();
        }
        this.addRowKey(rowKey);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.EdgeWriter#merge(org.plasma.sdo.PlasmaNode,
   * java.util.HashSet, java.util.List)
   */
  @Override
  public void merge(PlasmaNode dataNode, HashSet<PlasmaDataObject> oldValues,
      List<PlasmaEdge> currentEdges) throws IOException {
    if (this.collectionBaseType == null)
      if (oldValues.size() > 0)
        fetchMetadata();
      else
        findMetadata();
    HashMap<String, PlasmaDataObject> oldEdgeMap = new HashMap<>(oldValues.size());
    for (PlasmaDataObject dataObject : oldValues) {
      if (!dataObject.getDataGraph().getChangeSummary().isCreated(dataObject))
        oldEdgeMap.put(dataObject.getUUIDAsString(), dataObject);
    }
    Map<String, PlasmaDataObject> currentEdgeMap = toMap(dataNode, currentEdges);
    // remove old values not found in current
    Iterator<String> oldIter = oldEdgeMap.keySet().iterator();
    while (oldIter.hasNext()) {
      String key = oldIter.next();
      PlasmaDataObject oldValue = oldEdgeMap.get(key);
      if (!currentEdgeMap.containsKey(key)) {
        this.remove(oldValue);
      }
    }

    // add new/current values not found in old
    Iterator<String> newIter = currentEdgeMap.keySet().iterator();
    while (newIter.hasNext()) {
      String key = newIter.next();
      PlasmaDataObject newValue = currentEdgeMap.get(key);
      if (!oldEdgeMap.containsKey(key)) {
        this.add(newValue);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.EdgeWriter#write()
   */
  @Override
  public void write() throws IOException {

    // FIXME: determine whether to delete the edge(s) if all
    // have been removed
    if ((this.sequences == null || this.sequences.size() == 0)
        && (this.rowKeys == null || this.rowKeys.size() == 0)) {
      if (log.isDebugEnabled())
        log.debug("deleting edge, " + this);
      delete();
      return;
    }
    byte[] typeBytes = this.encodeCollectionBaseType();
    rowWriter.writeRowData(this.family, this.baseTypeQual, typeBytes);

    if (this.sequences != null && this.sequences.size() > 0) {
      byte[] sequenceBytes = this.encodeSequences();
      rowWriter.writeRowData(family, this.sequencesQual, sequenceBytes);
    }

    if (this.rowKeys != null && this.rowKeys.size() > 0) {
      byte[] rowKeysBytes = this.encodeRowKeys();
      rowWriter.writeRowData(this.family, this.keysQual, rowKeysBytes);
    }

    if (this.collectionDefaultSubType != null) {
      byte[] subTypesBytes = this.encodeDefaultSubType();
      rowWriter.writeRowData(family, this.subTypeQual, subTypesBytes);
    }

    byte[] countBytes = String.valueOf(this.getCount()).getBytes(charset);
    rowWriter.writeRowData(this.family, this.countQual, countBytes);
  }

  // Flag it deleted and write the data on exit
  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.EdgeWriter#delete()
   */
  @Override
  public void delete() throws IOException {
    rowWriter.deleteRowData(family, this.baseTypeQual);
    rowWriter.deleteRowData(family, this.subTypeQual);
    rowWriter.deleteRowData(family, this.pathQual);
    rowWriter.deleteRowData(family, this.sequencesQual);
    // rowWriter.deleteRowData(family, tableQual);
    rowWriter.deleteRowData(family, this.keysQual);
    rowWriter.deleteRowData(family, this.countQual);
  }

  private void addRowKey(byte[] rowKey) throws IOException {
    if (this.rowKeys == null)
      this.rowKeys = new ArrayList<KeyBytes>();
    this.rowKeys.add(new KeyBytes(rowKey));
  }

  private Map<String, PlasmaDataObject> toMap(PlasmaNode dataNode, List<PlasmaEdge> edges) {
    Map<String, PlasmaDataObject> result = new HashMap<>(edges.size());
    PlasmaDataObject opposite = null;
    for (PlasmaEdge edge : edges) {
      opposite = edge.getOpposite(dataNode).getDataObject();
      result.put(opposite.getUUIDAsString(), opposite);
    }
    return result;
  }

  /**
   * Return the sequence from the collection row writer or from the given data
   * object instance. NOTE: Used for modified and deleted data object only
   * 
   * @param dataObject
   *          the data object
   * @return the sequence
   */
  private Long getSequence(PlasmaDataObject dataObject) {
    Long sequence = null;
    if (this.rowWriter.containsSequence(dataObject)) {
      sequence = this.rowWriter.getSequence(dataObject);
      if (log.isDebugEnabled())
        log.debug("found existing sequence " + sequence + " for, " + dataObject);
    } else {
      CoreNode dataNode = ((CoreNode) dataObject);
      sequence = (Long) dataNode.getValue(CloudGraphConstants.SEQUENCE);
      if (sequence == null)
        throw new RequiredPropertyException("instance property '" + CloudGraphConstants.SEQUENCE
            + "' is required to update data object, " + dataObject);
      this.rowWriter.addSequence(dataObject, sequence);
      if (log.isDebugEnabled())
        log.debug("found existing instance property sequence " + sequence + " for, " + dataObject);
    }
    return sequence;
  }

  /**
   * Return the sequence from the collection row writer or create and return a
   * new sequence. NOTE: Used for created data object only
   * 
   * @param dataObject
   *          the data object
   * @return the sequence
   */
  private Long getOrCreateSequence(PlasmaDataObject dataObject) throws IOException {
    Long sequence = null;
    if (this.rowWriter.containsSequence(dataObject)) {
      sequence = this.rowWriter.getSequence(dataObject);
      if (log.isDebugEnabled())
        log.debug("found(2) existing sequence " + sequence + " for, " + dataObject);
    } else {
      sequence = rowWriter.newSequence(dataObject);
      this.rowWriter.addSequence(dataObject, sequence);
      if (log.isDebugEnabled())
        log.debug("created new sequence " + sequence + " for, " + dataObject);
    }
    return sequence;
  }
}
