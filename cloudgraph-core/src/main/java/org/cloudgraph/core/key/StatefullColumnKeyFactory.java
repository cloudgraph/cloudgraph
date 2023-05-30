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
package org.cloudgraph.core.key;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.Bytes;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.io.RowOperation;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.cloudgraph.store.key.MetaKey;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Generates an HBase column key based on the configured CloudGraph column key
 * {@link org.cloudgraph.store.mapping.ColumnKeyModel model} for a specific
 * HTable {@link org.cloudgraph.store.mapping.Table configuration}.
 * <p>
 * The initial creation and subsequent reconstitution for query retrieval
 * purposes of both row and column keys in CloudGraph&#8482; is efficient, as it
 * leverages byte array level API in both Java and the current underlying SDO
 * 2.1 implementation, <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a>. Both composite row and
 * column keys are composed in part of structural metadata, and the lightweight
 * metadata API within <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a> contains byte-array level,
 * cached lookup of all basic metadata elements including logical and physical
 * type and property names.
 * </p>
 * 
 * @see org.cloudgraph.store.mapping.ColumnKeyModel
 * @see org.cloudgraph.store.mapping.Table
 * @see org.cloudgraph.state.SequenceGenerator
 * @author Scott Cinnamond
 * @since 0.5
 */
public class StatefullColumnKeyFactory extends CompositeColumnKeyFactory implements
    GraphStatefullColumnKeyFactory {
  private static final Log log = LogFactory.getLog(StatefullColumnKeyFactory.class);

  public StatefullColumnKeyFactory(PlasmaType rootType, ServiceContext serviceContext) {
    super(rootType, serviceContext.getStoreMapping());
  }

  public StatefullColumnKeyFactory(RowOperation graphRow) {
    super(graphRow);
  }

  @Override
  public byte[] createColumnKey(PlasmaDataObject dataObject, long sequenceNum,
      PlasmaProperty property) {
    PlasmaType type = (PlasmaType) dataObject.getType();
    return getKey(type, sequenceNum, property);
  }

  @Override
  public byte[] createColumnKey(PlasmaDataObject dataObject, long sequenceNum,
      PlasmaProperty property, EdgeMetaKey metaField) {
    PlasmaType type = (PlasmaType) dataObject.getType();
    return getKey(type, sequenceNum, property, metaField);
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, long sequenceNum, PlasmaProperty property,
      EdgeMetaKey metaField) {
    return getKey(type, sequenceNum, property, metaField);
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, long sequenceNum, PlasmaProperty property,
      EntityMetaKey metaField) {
    return getKey(type, sequenceNum, property, metaField);
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, long sequenceNum, EntityMetaKey metaField) {
    return getKey(type, sequenceNum, metaField);
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, long dataObjectSeqNum, PlasmaProperty property) {
    return getKey(type, dataObjectSeqNum, property);
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, long sequenceNum, EdgeMetaKey metaField) {
    return getKey(type, sequenceNum, metaField);
  }

  @Override
  public byte[] createColumnKeyMetadataPrefix(PlasmaType type, long sequenceNum,
      PlasmaProperty property) {
    return getKeyMetadataPrefix(type, sequenceNum, property);
  }

  private byte[] getKey(PlasmaType type, long sequenceNum, PlasmaProperty property) {
    if (sequenceNum <= 0)
      throw new IllegalArgumentException("expected positive sequence number not " + sequenceNum);
    byte[] seqNumBytes = String.valueOf(sequenceNum).getBytes(this.charset);
    byte[] sectionDelim = this.getGraph().getColumnKeySequenceDelimiterBytes();
    byte[] prefix = super.createColumnKey(type, property);
    byte[] result = concat(prefix, sectionDelim, seqNumBytes);
    if (log.isDebugEnabled())
      log.debug("key: " + Bytes.toString(result));

    return result;
  }

  private byte[] getKeyMetadataPrefix(PlasmaType type, long sequenceNum, PlasmaProperty property) {
    if (sequenceNum <= 0)
      throw new IllegalArgumentException("expected positive sequence number not " + sequenceNum);
    byte[] seqNumBytes = String.valueOf(sequenceNum).getBytes(this.charset);
    byte[] sectionDelim = this.getGraph().getColumnKeySequenceDelimiterBytes();
    byte[] prefix = super.createColumnKey(type, property);
    byte[] metadataDelim = this.getGraph().getColumnKeyReferenceMetadataDelimiterBytes();
    byte[] result = concat(prefix, sectionDelim, seqNumBytes, metadataDelim);
    if (log.isDebugEnabled())
      log.debug("key: " + Bytes.toString(result));

    return result;
  }

  private byte[] getKey(PlasmaType type, long sequenceNum, PlasmaProperty property,
      MetaKey metaField) {
    if (sequenceNum <= 0)
      throw new IllegalArgumentException("expected positive sequence number not " + sequenceNum);
    byte[] seqNumBytes = String.valueOf(sequenceNum).getBytes(this.charset);
    byte[] sectionDelim = this.getGraph().getColumnKeySequenceDelimiterBytes();
    byte[] prefix = super.createColumnKey(type, property);
    byte[] metaDelim = this.getGraph().getColumnKeyReferenceMetadataDelimiterBytes();
    byte[] result = concat(prefix, sectionDelim, seqNumBytes, metaDelim, metaField.codeAsBytes());
    if (log.isDebugEnabled())
      log.debug("key: " + Bytes.toString(result));

    return result;
  }

  private byte[] getKey(PlasmaType type, long sequenceNum, EntityMetaKey metaField) {
    if (sequenceNum <= 0)
      throw new IllegalArgumentException("expected positive sequence number not " + sequenceNum);
    byte[] seqNumBytes = String.valueOf(sequenceNum).getBytes(this.charset);
    byte[] sectionDelim = this.getGraph().getColumnKeySequenceDelimiterBytes();

    byte[] prefix = super.createColumnKey(type, metaField);

    byte[] result = concat(prefix, sectionDelim, seqNumBytes);
    if (log.isDebugEnabled())
      log.debug("key: " + Bytes.toString(result));

    return result;
  }

  private byte[] getKey(PlasmaType type, long sequenceNum, EdgeMetaKey metaField) {
    if (sequenceNum <= 0)
      throw new IllegalArgumentException("expected positive sequence number not " + sequenceNum);
    byte[] seqNumBytes = String.valueOf(sequenceNum).getBytes(this.charset);
    byte[] sectionDelim = this.getGraph().getColumnKeySequenceDelimiterBytes();

    byte[] prefix = super.createColumnKey(type, metaField);

    byte[] result = concat(prefix, sectionDelim, seqNumBytes);
    if (log.isDebugEnabled())
      log.debug("key: " + Bytes.toString(result));

    return result;
  }

  private byte[] concat(byte[]... fields) {
    int i = 0;
    for (byte[] field : fields)
      i += field.length;
    byte[] result = new byte[i];
    i = 0;
    for (byte[] field : fields) {
      System.arraycopy(field, 0, result, i, field.length);
      i += field.length;
    }
    return result;
  }

}
