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
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.io.RowOperation;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.cloudgraph.store.mapping.MetaKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Creates an HBase column key based on the configured CloudGraph column key
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
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CompositeColumnKeyFactory extends ByteBufferKeyFactory implements
    GraphColumnKeyFactory {
  private static final Log log = LogFactory.getLog(CompositeColumnKeyFactory.class);

  public CompositeColumnKeyFactory(PlasmaType rootType, StoreMappingContext mappingContext) {
    super(rootType, mappingContext);
  }

  public CompositeColumnKeyFactory(RowOperation graphRow) {
    super(graphRow);
    // TODO Auto-generated constructor stub
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, PlasmaProperty property) {

    this.buf.clear();
    addColumnKeyFields(type, property);

    // ByteBuffer.array() returns unsized array so don't sent that back to
    // clients
    // to misuse.
    // Use native arraycopy() method as it uses native memcopy to create
    // result array
    // and because and
    // ByteBuffer.get(byte[] dst,int offset, int length) is not native
    byte[] result = new byte[this.buf.position()];
    System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0, this.buf.position());

    return result;
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, EntityMetaKey metaField) {
    this.buf.clear();
    addColumnKeyFields(type, metaField);
    byte[] result = new byte[this.buf.position()];
    System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0, this.buf.position());
    return result;
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, EdgeMetaKey metaField) {
    this.buf.clear();
    addColumnKeyFields(type, metaField);
    byte[] result = new byte[this.buf.position()];
    System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0, this.buf.position());
    return result;
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, PlasmaProperty property, EntityMetaKey metaField) {
    this.buf.clear();
    addColumnKeyFields(type, property);
    this.buf.put(this.getGraph().getColumnKeyReferenceMetadataDelimiterBytes());
    this.buf.put(metaField.codeAsBytes());
    byte[] result = new byte[this.buf.position()];
    System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0, this.buf.position());
    return result;
  }

  @Override
  public byte[] createColumnKey(PlasmaType type, PlasmaProperty property, EdgeMetaKey metaField) {
    this.buf.clear();
    addColumnKeyFields(type, property);
    this.buf.put(this.getGraph().getColumnKeyReferenceMetadataDelimiterBytes());
    this.buf.put(metaField.codeAsBytes());
    byte[] result = new byte[this.buf.position()];
    System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0, this.buf.position());
    return result;
  }

  protected void addColumnKeyFields(PlasmaType type, EntityMetaKey metaField) {
    int i = 0;
    for (KeyFieldMapping fieldConfig : this.getGraph().getColumnKeyFields()) {
      if (i > 0)
        this.buf.put(this.getGraph().getColumnKeyFieldDelimiterBytes());
      MetaKeyFieldMapping predefinedFieldConfig = (MetaKeyFieldMapping) fieldConfig;
      byte[] keyValue = predefinedFieldConfig.getKeyBytes(type, metaField);
      this.buf.put(keyValue);
      i++;
    }
  }

  protected void addColumnKeyFields(PlasmaType type, EdgeMetaKey metaField) {
    int i = 0;
    for (KeyFieldMapping fieldConfig : this.getGraph().getColumnKeyFields()) {
      if (i > 0)
        this.buf.put(this.getGraph().getColumnKeyFieldDelimiterBytes());
      MetaKeyFieldMapping predefinedFieldConfig = (MetaKeyFieldMapping) fieldConfig;
      byte[] keyValue = predefinedFieldConfig.getKeyBytes(type, metaField);
      this.buf.put(keyValue);
      i++;
    }
  }

  protected void addColumnKeyFields(PlasmaType type, PlasmaProperty property) {
    int i = 0;
    for (KeyFieldMapping fieldConfig : this.getGraph().getColumnKeyFields()) {
      if (i > 0)
        this.buf.put(this.getGraph().getColumnKeyFieldDelimiterBytes());
      MetaKeyFieldMapping predefinedFieldConfig = (MetaKeyFieldMapping) fieldConfig;
      byte[] keyValue = predefinedFieldConfig.getKeyBytes(type, property);
      this.buf.put(keyValue);
      i++;
    }
  }

  // protected byte[] configureTokenBytes(byte[] token, DataGraphMapping graph,
  // Hashing hashing,
  // PreDefinedFieldName tokenName) {
  // byte[] result = token;
  // ColumnKeyFieldMapping tokenConfig = graph.getColumnKeyField(tokenName);
  // if (tokenConfig != null) {
  // if (tokenConfig.isHash()) {
  // result = hashing.toStringBytes(result);
  // }
  // }
  // return result;
  // }
  //
}
