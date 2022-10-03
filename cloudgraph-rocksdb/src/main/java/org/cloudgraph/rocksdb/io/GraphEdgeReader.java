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
package org.cloudgraph.rocksdb.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.rocksdb.graph.CellConverter;
import org.cloudgraph.rocksdb.key.CompositeRowKeyReader;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

public class GraphEdgeReader extends DefaultEdgeOperation implements EdgeReader {
  private static Log log = LogFactory.getLog(GraphEdgeReader.class);
  private PlasmaType oppositeType;
  private boolean oppositeTypeBound;
  private RowReader rowReader;

  GraphEdgeReader(PlasmaType sourceType, PlasmaProperty sourceProp, Long typeSequenceNum,
      TableMapping tableConfig, DataGraphMapping graphConfig, RowReader rowReader)
      throws IOException {

    super(sourceType, sourceProp, tableConfig, graphConfig, rowReader);

    this.rowReader = rowReader;
    this.family = tableConfig.getDataColumnFamilyNameBytes();
    this.baseTypeQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType,
        typeSequenceNum, sourceProp, EdgeMetaKey.BASETYPE);
    this.pathQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType,
        typeSequenceNum, sourceProp, EdgeMetaKey.PATH);
    this.sequencesQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType,
        typeSequenceNum, sourceProp, EdgeMetaKey.SEQUENCES);
    this.subTypeQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType,
        typeSequenceNum, sourceProp, EdgeMetaKey.SUBTYPE);
    // this.tableQual = rowReader.getColumnKeyFactory().createColumnKey(
    // this.sourceType, typeSequenceNum, sourceProp, EdgeMetaField._TBL_);
    this.keysQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType,
        typeSequenceNum, sourceProp, EdgeMetaKey.ROWKEYS);
    this.countQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType,
        typeSequenceNum, sourceProp, EdgeMetaKey.COUNT);

    this.typeQualStr = new String(this.baseTypeQual, charset);
    this.subTypeQualStr = new String(this.subTypeQual, charset);
    this.pathQualStr = new String(this.pathQual, charset);
    this.sequencesQualStr = new String(this.sequencesQual, charset);
    // this.tableQualStr = new String(this.tableQual, this.charset);
    this.keysQualStr = new String(this.keysQual, charset);
    this.countQualStr = new String(this.countQual, charset);

    this.baseTypeQualDataType = EdgeMetaKey.BASETYPE.getStorageType();
    this.subTypeQualDataType = EdgeMetaKey.SUBTYPE.getStorageType();
    this.pathQualDataType = EdgeMetaKey.PATH.getStorageType();
    this.sequencesQualDataType = EdgeMetaKey.SEQUENCES.getStorageType();
    // protected byte[] tableQual;
    this.keysQualDataType = EdgeMetaKey.ROWKEYS.getStorageType();
    this.countQualDataType = EdgeMetaKey.COUNT.getStorageType();

    this.construct(rowReader, graphConfig);
  }

  GraphEdgeReader(PlasmaType targetType, PlasmaProperty sourceProp, TableMapping tableConfig,
      DataGraphMapping graphConfig, RowReader rowReader) throws IOException {
    super(targetType, sourceProp, tableConfig, graphConfig, rowReader);

    this.rowReader = rowReader;
    this.family = tableConfig.getDataColumnFamilyNameBytes();
    this.baseTypeQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType,
        sourceProp, EdgeMetaKey.BASETYPE);
    this.pathQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType, sourceProp,
        EdgeMetaKey.PATH);
    this.sequencesQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType,
        sourceProp, EdgeMetaKey.SEQUENCES);
    this.subTypeQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType, sourceProp,
        EdgeMetaKey.SUBTYPE);
    // this.tableQual = rowReader.getColumnKeyFactory().createColumnKey(
    // this.sourceType, sourceProp, EdgeMetaField._TBL_);
    this.keysQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType, sourceProp,
        EdgeMetaKey.ROWKEYS);
    this.countQual = rowReader.getColumnKeyFactory().createColumnKey(this.sourceType, sourceProp,
        EdgeMetaKey.COUNT);

    this.typeQualStr = new String(this.baseTypeQual, charset);
    this.subTypeQualStr = new String(this.subTypeQual, charset);
    this.pathQualStr = new String(this.pathQual, charset);
    this.sequencesQualStr = new String(this.sequencesQual, charset);
    // this.tableQualStr = new String(this.tableQual, this.charset);
    this.keysQualStr = new String(this.keysQual, charset);
    this.countQualStr = new String(this.countQual, charset);

    this.baseTypeQualDataType = EdgeMetaKey.BASETYPE.getStorageType();
    this.subTypeQualDataType = EdgeMetaKey.SUBTYPE.getStorageType();
    this.pathQualDataType = EdgeMetaKey.PATH.getStorageType();
    this.sequencesQualDataType = EdgeMetaKey.SEQUENCES.getStorageType();
    // protected byte[] tableQual;
    this.keysQualDataType = EdgeMetaKey.ROWKEYS.getStorageType();
    this.countQualDataType = EdgeMetaKey.COUNT.getStorageType();

    this.construct(rowReader, graphConfig);
  }

  static boolean exists(PlasmaType targetType, PlasmaProperty sourceProp, TableMapping tableConfig,
      DataGraphMapping graphConfig, RowReader rowReader) throws IOException {
    byte[] fam = tableConfig.getDataColumnFamilyNameBytes();
    byte[] typeQual = rowReader.getColumnKeyFactory().createColumnKey(targetType, sourceProp,
        EdgeMetaKey.BASETYPE);
    return rowReader.getRow().containsColumn(fam, typeQual);
  }

  static boolean exists(PlasmaType targetType, PlasmaProperty sourceProp, Long typeSequenceNum,
      TableMapping tableConfig, DataGraphMapping graphConfig, RowReader rowReader)
      throws IOException {
    byte[] fam = tableConfig.getDataColumnFamilyNameBytes();
    byte[] typeQual = rowReader.getColumnKeyFactory().createColumnKey(targetType, typeSequenceNum,
        sourceProp, EdgeMetaKey.BASETYPE);
    return rowReader.getRow().containsColumn(fam, typeQual);
  }

  private void construct(RowReader rowReader, DataGraphMapping graphConfig) {
    log.debug("construct: " + this.toString());

    this.oppositeType = (PlasmaType) sourceProp.getType();
    this.oppositeTypeBound = StoreMapping.getInstance().findTable(
        this.oppositeType.getQualifiedName(), this.rowOperation.getMappingContext()) != null;

    byte[] typeBytes = fetchValue(family, baseTypeQual, rowReader.getRow());
    decodeCollectionBaseType(typeBytes);

    byte[] pathBytes = findValue(family, pathQual, rowReader.getRow());
    if (pathBytes != null)
      this.collectionPath = new String(pathBytes, this.charset);

    byte[] countBytes = fetchValue(family, countQual, rowReader.getRow());
    long count = Long.valueOf(new String(countBytes, charset));

    byte[] sequenceBytes = findValue(family, sequencesQual, rowReader.getRow());
    if (sequenceBytes != null) {
      if (log.isDebugEnabled()) {
        log.debug("decoding sequences: " + new String(sequenceBytes, charset));
      }
      this.decodeSequences(sequenceBytes);
      if (count != this.sequences.size())
        throw new IllegalStateException("invalid value " + count + " for qualifier "
            + EdgeMetaKey.COUNT + " (" + Bytes.toString(countQual) + ") for key: '"
            + Bytes.toString(rowReader.getRowKey()) + "' - expected " + this.sequences.size());
    }

    // byte[] tableBytes = findValue(family, tableQual, rowReader.getRow());
    // if (tableBytes != null)
    // this.table = new String(tableBytes, this.charset);

    byte[] rowKeyBytes = findValue(family, keysQual, rowReader.getRow());
    if (rowKeyBytes != null) {
      if (log.isDebugEnabled()) {
        log.debug("decoding row keys: " + new String(rowKeyBytes, charset));
      }
      this.decodeRowKeys(rowKeyBytes);
      if (count != this.rowKeys.size())
        throw new IllegalStateException("invalid value " + count + " for qualifier "
            + EdgeMetaKey.COUNT + " (" + Bytes.toString(countQual) + ") for key: '"
            + Bytes.toString(rowReader.getRowKey()) + "' - expected " + this.rowKeys.size());
    }

    byte[] subtypesBytes = findValue(family, subTypeQual, rowReader.getRow());
    if (subtypesBytes != null) {
      this.decodeDefaultSubType(subtypesBytes);
    }

  }

  @Override
  public List<CellValues> getRowValues() {
    List<CellValues> result = new ArrayList<>(this.getRowKeys().size());
    PlasmaType subType = this.collectionBaseType;
    if (this.getSubType() != null)
      subType = this.getSubType();
    CompositeRowKeyReader rowKeyReader = new CompositeRowKeyReader(subType,
        this.rowOperation.getMappingContext());
    CellConverter cellConverter = new CellConverter(subType, this.tableConfig,
        this.rowOperation.getMappingContext());
    for (KeyBytes rowKeyBYtes : this.getRowKeys()) {
      rowKeyReader.read(rowKeyBYtes.getKey());
      result.add(cellConverter.convert(rowKeyBYtes.getKey(), rowKeyReader.getValues()));
    }

    return result;
  }

  @Override
  public RowReader getRowReader() {
    return this.rowReader;
  }

  @Override
  public TableReader getTableReader() {
    return this.rowReader.getTableReader();
  }

}
