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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.state.StateException;
import org.cloudgraph.state.proto.RowKeysProto.RowKey;
import org.cloudgraph.state.proto.RowKeysProto.RowKeys;
import org.cloudgraph.store.key.RequiredKeyFieldException;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.helper.PlasmaTypeHelper;

public abstract class DefaultEdgeOperation implements EdgeOperation {
  private static Log log = LogFactory.getLog(DefaultEdgeOperation.class);

  /** internal delimiter for metadta values */
  protected static String TYPE_DELIM = "|";
  protected static String TYPE_DELIM_REGEXP = "\\|";

  protected PlasmaType sourceType;
  protected PlasmaProperty sourceProp;
  protected TableConfig tableConfig;
  protected DataGraphConfig graphConfig;
  protected static Charset charset = Charset.forName("UTF-8");

  protected PlasmaType collectionBaseType;
  protected PlasmaType collectionDefaultSubType;
  protected String collectionPath;
  protected List<Long> sequences;
  // protected String table;
  protected List<String> rowKeys;

  protected byte[] family;
  protected byte[] columnKeyFeildDelim;
  protected String columnKeyFeildDelimRegexp;

  protected byte[] baseTypeQual;
  protected byte[] subTypeQual;
  protected byte[] pathQual;
  protected byte[] sequencesQual;
  // protected byte[] tableQual;
  protected byte[] keysQual;
  protected byte[] countQual;

  protected String typeQualStr;
  protected String pathQualStr;
  protected String sequencesQualStr;
  // protected String tableQualStr;
  protected String keysQualStr;
  protected String countQualStr;

  protected DefaultEdgeOperation(PlasmaType sourceType, PlasmaProperty sourceProp,
      TableConfig tableConfig, DataGraphConfig graphConfig) throws IOException {
    super();
    this.sourceType = sourceType;
    this.sourceProp = sourceProp;
    this.tableConfig = tableConfig;
    this.graphConfig = graphConfig;
    charset = tableConfig.getCharset();

    this.family = tableConfig.getDataColumnFamilyNameBytes();

    this.columnKeyFeildDelim = graphConfig.getColumnKeyFieldDelimiterBytes();
    this.columnKeyFeildDelimRegexp = graphConfig.getColumnKeyFieldDelimiter();
    if (this.columnKeyFeildDelimRegexp.length() == 1) {
      char c = this.columnKeyFeildDelimRegexp.charAt(0);
      if (isSpecial(c)) {
        columnKeyFeildDelimRegexp = "\\" + String.valueOf(c);
      }
    }
  }

  public PlasmaType getTargetType() {
    return sourceType;
  }

  public PlasmaProperty getSourceProp() {
    return sourceProp;
  }

  private boolean isSpecial(char c) {
    switch (c) {
    case '|':
    case '*':
      return true;
    default:
      return false;
    }
  }

  /**
   * Calculates and returns whether the edges are local. If the opposite type is
   * not bound (to a table), the type is stored in the local row and we write
   * its sequences for this edge. Or if this type is not bound to a table, we
   * also know its opposite edges must be local as well.
   * 
   * @return whether the edges are local
   */
  // protected boolean localEdges() {
  // //return !this.oppositeTypeBound || !this.thisTypeBound;
  // return !this.oppositeTypeBound;
  // }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.KeyMeta#getBaseType()
   */
  @Override
  public PlasmaType getBaseType() {
    return collectionBaseType;
  }

  @Override
  public PlasmaType getSubType() {
    return this.collectionDefaultSubType;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.KeyMeta#getCollectionPath()
   */
  @Override
  public String getPath() {
    return collectionPath;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.KeyMeta#getSequences()
   */
  @Override
  public List<Long> getSequences() {
    return sequences;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.KeyMeta#getTable()
   */
  @Override
  public String getTable() {
    if (!isExternal())
      throw new IllegalStateException("cannot find table for local (non-external) edge, " + this);

    TableConfig table = null;
    if (this.collectionDefaultSubType != null) {
      table = CloudGraphConfig.getInstance().findTable(
          this.collectionDefaultSubType.getQualifiedName());
    } else {
      table = CloudGraphConfig.getInstance().findTable(this.collectionBaseType.getQualifiedName());
    }
    return table.getName();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.KeyMeta#getRowKeys()
   */
  @Override
  public List<String> getRowKeys() {
    return rowKeys;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.KeyMeta#isExternal()
   */
  private Boolean edgeTypeBound;

  @Override
  public boolean isExternal() {
    if (edgeTypeBound != null) {
      return edgeTypeBound;
    } else {
      boolean result = false;
      if (this.collectionDefaultSubType != null) {
        result = CloudGraphConfig.getInstance().findTable(
            this.collectionDefaultSubType.getQualifiedName()) != null;
      } else {
        edgeTypeBound = CloudGraphConfig.getInstance().findTable(
            this.collectionBaseType.getQualifiedName()) != null;
        result = edgeTypeBound;
      }
      return result;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.hbase.io.KeyMeta#getCount()
   */
  @Override
  public long getCount() {
    if (this.sequences != null && this.sequences.size() > 0)
      return this.sequences.size();
    else if (this.rowKeys != null && this.rowKeys.size() > 0)
      return this.rowKeys.size();
    else
      return 0;
  }

  protected byte[] encodeRowKeys() {
    RowKeys.Builder rowKeysBuilder = RowKeys.newBuilder();
    for (String key : rowKeys) {
      RowKey.Builder rowKeyBuilder = RowKey.newBuilder();
      rowKeyBuilder.setKey(key);
      rowKeysBuilder.addRowKey(rowKeyBuilder);
    }
    RowKeys keys = rowKeysBuilder.build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] bytes = null;
    try {
      keys.writeDelimitedTo(baos);
      baos.flush();
      bytes = baos.toByteArray();
    } catch (IOException e) {
      throw new StateException(e);
    } finally {
      try {
        baos.close();
      } catch (IOException e) {
      }
    }
    return bytes;
  }

  protected void decodeRowKeys(byte[] rowKeys) {
    ByteArrayInputStream bais = new ByteArrayInputStream(rowKeys);
    try {
      RowKeys.Builder result = RowKeys.newBuilder();
      result.mergeDelimitedFrom(bais);
      RowKeys keys = result.build();
      this.rowKeys = new ArrayList<>(keys.getRowKeyCount());
      for (RowKey rowKey : keys.getRowKeyList())
        this.rowKeys.add(rowKey.getKey());
    } catch (IOException e) {
      throw new StateException(e);
    } finally {
      try {
        bais.close();
      } catch (IOException e) {
      }
    }
  }

  protected byte[] encodeSequences() {
    StringBuilder buf = new StringBuilder();
    int i = 0;
    for (Long seq : sequences) {
      if (i > 0)
        buf.append(' ');
      buf.append(String.valueOf(seq));
      i++;
    }
    return buf.toString().getBytes(charset);
  }

  protected void decodeSequences(byte[] sequenceBytes) {
    String[] tokens = (new String(sequenceBytes, charset)).split(" ");
    sequences = new ArrayList<Long>(tokens.length);
    for (int i = 0; i < tokens.length; i++)
      sequences.add(Long.parseLong(tokens[i]));
  }

  // protected byte[] encodeSubType() {
  // //FIXME: pre-allocate capacity according to collection
  // ByteBuffer buf = ByteBuffer.allocate(6000);
  // Iterator<Long> iter = this.subTypeMap.keySet().iterator();
  // int i = 0;
  // while (iter.hasNext()) {
  // if (i > 0)
  // buf.put(" ".getBytes(charset));
  // Long seq = iter.next();
  // PlasmaType type = this.subTypeMap.get(seq);
  // byte[] typeBytes = encodeType(type);
  // byte[] subtypeBytes =
  // org.cloudgraph.common.Bytes.concat(String.valueOf(seq).getBytes(charset),
  // TYPE_DELIM.getBytes(charset), typeBytes);
  // buf.put(subtypeBytes);
  // i++;
  // }
  // byte[] result = new byte[buf.position()];
  // System.arraycopy(buf.array(), buf.arrayOffset(), result, 0,
  // buf.position());
  // return result;
  // }
  //
  // protected void decodeSubType(byte[] sequenceBytes) {
  // String[] elements = (new String(sequenceBytes, charset)).split(" ");
  // this.subTypeMap = new HashMap<>();
  // for (int i = 0; i < elements.length; i++) {
  // String[] tokens = elements[i].split(TYPE_DELIM_REGEXP);
  // Long seq = Long.parseLong(tokens[0]);
  // PlasmaType type =
  // (PlasmaType)PlasmaTypeHelper.INSTANCE.getType(tokens[1], tokens[2]);
  // this.subTypeMap.put(seq, type);
  // }
  // }

  protected byte[] encodeDefaultSubType() {
    return encodeType(this.collectionDefaultSubType);
  }

  protected void decodeDefaultSubType(byte[] typeBytes) {
    String[] tokens = (new String(typeBytes, this.charset)).split(this.columnKeyFeildDelimRegexp);
    this.collectionDefaultSubType = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(tokens[0],
        tokens[1]);
  }

  protected byte[] encodeCollectionBaseType() {
    return encodeType(this.collectionBaseType);
  }

  protected void decodeCollectionBaseType(byte[] typeBytes) {
    String[] tokens = (new String(typeBytes, this.charset)).split(this.columnKeyFeildDelimRegexp);
    this.collectionBaseType = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(tokens[0], tokens[1]);
  }

  private byte[] encodeType(PlasmaType type) {
    byte[] uriPhysicalName = type.getURIPhysicalNameBytes();
    byte[] uri = type.getURIBytes();
    byte[] physicalName = type.getPhysicalNameBytes();
    byte[] name = type.getNameBytes();
    if (uriPhysicalName != null && physicalName != null) {
      return org.cloudgraph.common.Bytes.concat(uriPhysicalName, TYPE_DELIM.getBytes(charset),
          physicalName);
    } else {
      log.warn("no qualified physical name available for type, "
          + type
          + ", encoding qualified logical name - please annotate your model with physical name aliases to facilitate logical/physical name isolation");
      return org.cloudgraph.common.Bytes.concat(uri, TYPE_DELIM.getBytes(charset), name);
    }
  }

  public static PlasmaType decodeType(byte[] typeBytes) {
    String[] tokens = (new String(typeBytes, charset)).split(TYPE_DELIM_REGEXP);
    PlasmaType result = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(tokens[0], tokens[1]);
    return result;
  }

  private byte[] concat(byte[]... fields) {
    return org.cloudgraph.common.Bytes.concat(fields);
  }

  protected byte[] fetchValue(byte[] family, byte[] qualifier, CellValues row) {
    if (!row.containsColumn(family, qualifier)) {
      throw new RequiredKeyFieldException("expected column qualifier '" + new String(qualifier)
          + "' for reference property, " + sourceProp);
    }

    return row.getColumnValue(family, qualifier);
  }

  protected byte[] findValue(byte[] family, byte[] qualifier, CellValues row) {
    if (!row.containsColumn(family, qualifier)) {
      return null;
    }

    return row.getColumnValue(family, qualifier);
  }

  protected byte[] fetchValue(byte[] family, byte[] qualifier, Result row) {
    if (!row.containsColumn(family, qualifier)) {
      throw new RequiredKeyFieldException("expected column qualifier '" + new String(qualifier)
          + "' for reference property, " + sourceProp);
    }

    return row.getValue(family, qualifier);
  }

  protected byte[] findValue(byte[] family, byte[] qualifier, Result row) {
    if (!row.containsColumn(family, qualifier)) {
      return null;
    }

    return row.getValue(family, qualifier);
  }

  protected byte[] fetchValue(byte[] family, byte[] qualifier, Put row) {
    if (!row.has(family, qualifier)) {
      throw new RequiredKeyFieldException("expected column qualifier '" + new String(qualifier)
          + "' for reference property, " + sourceProp);
    }
    List<Cell> cells = row.get(family, qualifier);
    Cell last = cells.get(cells.size() - 1);
    return CellUtil.cloneValue(last);
  }

  protected byte[] findValue(byte[] family, byte[] qualifier, Put row) {
    if (!row.has(family, qualifier)) {
      return null;
    }
    List<Cell> cells = row.get(family, qualifier);
    Cell last = cells.get(cells.size() - 1);
    return CellUtil.cloneValue(last);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(this.sourceProp);
    return buf.toString();
  }
}
