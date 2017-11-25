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
package org.cloudgraph.hbase.key;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.state.RowState;
import org.cloudgraph.store.key.GraphRowKeyFactory;
import org.cloudgraph.store.key.KeyFieldOverflowException;
import org.cloudgraph.store.key.KeyValue;
import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.cloudgraph.store.mapping.PreDefinedKeyFieldMapping;
import org.cloudgraph.store.mapping.UserDefinedRowKeyFieldMapping;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaProperty;
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
  protected Padding padding;

  public CompositeRowKeyFactory(RowState graphRow) {
    super(graphRow);
    this.padding = new Padding(this.charset);
  }

  public CompositeRowKeyFactory(PlasmaType rootType) {
    super(rootType);
    this.padding = new Padding(this.charset);
  }

  /**
   * Creates a row key using only the given type information. The key is
   * therefore composed of only "metadata" fields which are pre-defined.
   * 
   * @param type
   *          the data object type
   * @return the row key
   */
  public String createRowKey(Type type) {
    StringBuilder result = new StringBuilder();
    PlasmaType plasmaType = (PlasmaType) type;

    List<PreDefinedKeyFieldMapping> preDefinedFields = this.getGraph().getPreDefinedRowKeyFields();
    for (int i = 0; i < preDefinedFields.size(); i++) {
      PreDefinedKeyFieldMapping preDefinedField = preDefinedFields.get(i);
      if (i > 0)
        result.append(this.getGraph().getRowKeyFieldDelimiter());
      String tokenValue = this.keySupport.getPredefinedFieldValue(plasmaType, this.hashing,
          preDefinedField);

      String padded = this.padding.pad(tokenValue, preDefinedField.getMaxLength(),
          preDefinedField.getDataFlavor());
      result.append(padded);
    }
    return result.toString();
  }

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
    List<PreDefinedKeyFieldMapping> preDefinedFields = this.getGraph().getPreDefinedRowKeyFields();
    for (int i = 0; i < preDefinedFields.size(); i++) {
      PreDefinedKeyFieldMapping preDefinedField = preDefinedFields.get(i);
      if (i > 0)
        this.buf.put(this.getGraph().getRowKeyFieldDelimiterBytes());
      byte[] tokenValue = this.keySupport.getPredefinedFieldValueBytes(type, this.hashing,
          preDefinedField);
      if (preDefinedField.isHash()) {
        tokenValue = padding.pad(tokenValue, preDefinedField.getMaxLength(), DataFlavor.integral);
      } else {
        tokenValue = padding.pad(tokenValue, preDefinedField.getMaxLength(),
            preDefinedField.getDataFlavor());
      }

      this.buf.put(tokenValue);
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

      byte[] keyValue = fieldConfig.getKeyBytes(rootDataObject);
      byte[] paddedKeyValue = null;
      if (fieldConfig.isHash()) {
        keyValue = this.hashing.toStringBytes(keyValue);
        paddedKeyValue = padding.pad(keyValue, fieldConfig.getMaxLength(), DataFlavor.integral);
      } else {
        if (fieldConfig instanceof UserDefinedRowKeyFieldMapping) {
          UserDefinedRowKeyFieldMapping userField = (UserDefinedRowKeyFieldMapping) fieldConfig;
          PlasmaProperty endpointProp = userField.getEndpointProperty();
          int delta = userField.getMaxLength() - keyValue.length;
          if (delta < 0)
            throw new KeyFieldOverflowException("user-defined field value '"
                + new String(keyValue, this.charset) + "' for path endpoint (property), "
                + endpointProp + ", with dataflavor, " + endpointProp.getDataFlavor()
                + ", exceeded max length (" + String.valueOf(userField.getMaxLength()) + ")");
          paddedKeyValue = padding.pad(keyValue, userField.getMaxLength(),
              endpointProp.getDataFlavor());
        } else {
          paddedKeyValue = padding.pad(keyValue, fieldConfig.getMaxLength(),
              fieldConfig.getDataFlavor());
        }
      }

      this.buf.put(paddedKeyValue);

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

    byte[] fieldValue = null;
    int i = 0;
    for (KeyFieldMapping fieldConfig : this.getGraph().getRowKeyFields()) {
      if (i > 0)
        this.buf.put(this.getGraph().getRowKeyFieldDelimiterBytes());

      if (fieldConfig instanceof PreDefinedKeyFieldMapping) {
        PreDefinedKeyFieldMapping predefinedConfig = (PreDefinedKeyFieldMapping) fieldConfig;
        fieldValue = predefinedConfig.getKeyBytes(this.getRootType());
      } else {
        UserDefinedRowKeyFieldMapping userFieldConfig = (UserDefinedRowKeyFieldMapping) fieldConfig;
        KeyValue keyValue = this.keySupport.findKeyValue(userFieldConfig, values);

        if (keyValue != null) {

          // FIXME: do we want to invoke a converter here?
          // FIXME: do we want to transform this value somehow?
          String stringValue = String.valueOf(keyValue.getValue());
          fieldValue = stringValue.getBytes(this.charset);

        } else {
          continue; // could be a partial row key scan
        }
      }

      byte[] paddedKeyValue = null;
      if (fieldConfig.isHash()) {
        fieldValue = this.hashing.toStringBytes(fieldValue);
        paddedKeyValue = padding.pad(fieldValue, fieldConfig.getMaxLength(), DataFlavor.integral);
      } else {
        if (fieldConfig instanceof UserDefinedRowKeyFieldMapping) {
          UserDefinedRowKeyFieldMapping userField = (UserDefinedRowKeyFieldMapping) fieldConfig;
          PlasmaProperty endpointProp = userField.getEndpointProperty();
          int delta = userField.getMaxLength() - fieldValue.length;
          if (delta < 0)
            throw new KeyFieldOverflowException("user-defined field value '"
                + new String(fieldValue, this.charset) + "' for path endpoint (property), "
                + endpointProp + ", with dataflavor, " + endpointProp.getDataFlavor()
                + ", exceeded max length (" + String.valueOf(userField.getMaxLength()) + ")");

          paddedKeyValue = padding.pad(fieldValue, userField.getMaxLength(), userField
              .getEndpointProperty().getDataFlavor());
        } else {
          paddedKeyValue = padding.pad(fieldValue, fieldConfig.getMaxLength(),
              fieldConfig.getDataFlavor());
        }
      }

      this.buf.put(paddedKeyValue);
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

}
