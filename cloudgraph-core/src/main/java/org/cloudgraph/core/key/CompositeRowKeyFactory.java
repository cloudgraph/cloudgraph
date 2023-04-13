/** * Copyright 2017 TerraMeta Software, Inc.
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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.Bytes;
import org.cloudgraph.core.ServiceContext;
//import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.core.io.RowOperation;
import org.cloudgraph.store.key.GraphRowKeyFactory;
import org.cloudgraph.store.key.KeyFieldOverflowException;
import org.cloudgraph.store.key.KeyValue;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.KeyFieldMapping;
//import org.cloudgraph.store.mapping.Padding;
import org.cloudgraph.store.mapping.MetaKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;
import commonj.sdo.Type;

/**
 * Generates an HBase row key based on the configured CloudGraph
 * {@link org.cloudgraph.store.mapping.RowKeyModel Row Key Model} for a specific
 * {@link org.cloudgraph.store.mapping.Table HTable Configuration}.
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
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CompositeRowKeyFactory extends ByteBufferKeyFactory implements GraphRowKeyFactory {
  private static final Log log = LogFactory.getLog(CompositeRowKeyFactory.class);

  public CompositeRowKeyFactory(RowOperation graphRow) {
    super(graphRow);
  }

  public CompositeRowKeyFactory(PlasmaType rootType, ServiceContext serviceContext) {
    super(rootType, serviceContext);
  }

  /**
   * Creates a row key using only the given type information. The key is
   * therefore composed of only "metadata" fields which are pre-defined.
   * 
   * @param type
   *          the data object type
   * @return the row key
   */
  @Override
  public byte[] createRowKeyBytes(Type type) {

    PlasmaType plasmaType = (PlasmaType) type;

    this.buf.clear();

    try {
      create(plasmaType);
    } catch (BufferOverflowException e) {
      this.bufsize = this.bufsize * 2;
      this.buf = ByteBuffer.allocate(this.bufsize);
      create(plasmaType);
    }

    return this.buf.array();
  }

  private void create(PlasmaType type) {
    List<MetaKeyFieldMapping> preDefinedFields = this.getGraph().getPreDefinedRowKeyFields();
    for (int i = 0; i < preDefinedFields.size(); i++) {
      MetaKeyFieldMapping preDefinedField = preDefinedFields.get(i);
      if (i > 0)
        this.buf.put(this.getGraph().getRowKeyFieldDelimiterBytes());

      Object keyValue = preDefinedField.getKey(type);
      byte[] encodedKeyValue = preDefinedField.getCodec().encode(keyValue);
      if (!preDefinedField.getCodec().isTransforming()
          && preDefinedField.getCodec().checkEncodeOverflow(encodedKeyValue))
        throw new KeyFieldOverflowException("value '" + Bytes.toString(encodedKeyValue)
            + "' exceeds capacity for key field: " + preDefinedField);

      // byte[] tokenValue = this.keySupport.getEncodedPredefinedField(type, //
      // this.hashing,
      // preDefinedField);

      this.buf.put(encodedKeyValue);
    }
  }

  @Override
  public byte[] createRowKeyBytes(DataGraph dataGraph) {
    return createRowKeyBytes(dataGraph.getRootObject());
  }

  @Override
  public byte[] createRowKeyBytes(DataObject rootDataObject) {

    this.buf.clear();

    int i = 0;
    for (KeyFieldMapping fieldConfig : this.getGraph().getRowKeyFields()) {
      if (i > 0)
        this.buf.put(this.getGraph().getRowKeyFieldDelimiterBytes());

      Object keyValue = fieldConfig.getKey(rootDataObject);
      byte[] encodedKeyValue = fieldConfig.getCodec().encode(keyValue);
      if (!fieldConfig.getCodec().isTransforming()
          && fieldConfig.getCodec().checkEncodeOverflow(encodedKeyValue))
        throw new KeyFieldOverflowException("value '" + Bytes.toString(encodedKeyValue)
            + "' exceeds capacity for key field: " + fieldConfig);
      this.buf.put(encodedKeyValue);

      i++;
    }

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
  public byte[] createRowKeyBytes(List<KeyValue> values) {
    this.buf.clear();

    Object fieldValue = null;
    int i = 0;
    for (KeyFieldMapping fieldConfig : this.getGraph().getRowKeyFields()) {
      if (i > 0)
        this.buf.put(this.getGraph().getRowKeyFieldDelimiterBytes());

      if (fieldConfig instanceof MetaKeyFieldMapping) {
        MetaKeyFieldMapping predefinedConfig = (MetaKeyFieldMapping) fieldConfig;
        fieldValue = predefinedConfig.getKey(this.getRootType());
      } else {
        DataRowKeyFieldMapping userFieldConfig = (DataRowKeyFieldMapping) fieldConfig;
        KeyValue keyValue = this.keySupport.findKeyValue(userFieldConfig, values);

        if (keyValue != null) {
          fieldValue = keyValue.getValue();
        } else {
          continue; // could be a partial row key scan
        }
      }

      byte[] encodedKeyValue = null;

      switch (fieldConfig.getCodecType()) {
      case HASH:
        encodedKeyValue = fieldConfig.getCodec().encode(fieldValue);
        break;
      default:
        encodedKeyValue = fieldConfig.getCodec().encode(fieldValue);
        break;
      }
      if (!fieldConfig.getCodec().isTransforming()
          && fieldConfig.getCodec().checkEncodeOverflow(encodedKeyValue))
        throw new KeyFieldOverflowException("value '" + Bytes.toString(encodedKeyValue)
            + "' exceeds capacity for key field: " + fieldConfig);

      this.buf.put(encodedKeyValue);
      i++;
    }

    // ByteBuffer.array() returns unsized array so don't send that back to
    // clients to misuse.
    // Use native arraycopy() method as it uses native memcopy to create
    // result array
    // and because and
    // ByteBuffer.get(byte[] dst,int offset, int length) is not native
    byte[] result = new byte[this.buf.position()];
    System.arraycopy(this.buf.array(), this.buf.arrayOffset(), result, 0, this.buf.position());

    return result;
  }

}
