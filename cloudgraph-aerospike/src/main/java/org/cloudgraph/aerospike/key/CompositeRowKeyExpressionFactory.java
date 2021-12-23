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
package org.cloudgraph.aerospike.key;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.aerospike.io.RowOperation;
import org.cloudgraph.state.RowState;
import org.cloudgraph.store.key.GraphKeyException;
import org.cloudgraph.store.key.GraphRowKeyExpressionFactory;
import org.cloudgraph.store.key.KeyValue;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.cloudgraph.store.mapping.MetaKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Type;

/**
 * Generates an HBase row key based on the configured CloudGraph
 * {@link org.cloudgraph.store.mapping.RowKeyModel Row Key Model} for a specific
 * {@link org.cloudgraph.store.mapping.Table HTable Configuration}.
 * <p>
 * The initial creation and subsequent re-constitution for query retrieval
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
public class CompositeRowKeyExpressionFactory extends ByteBufferKeyFactory implements
    GraphRowKeyExpressionFactory {
  private static final Log log = LogFactory.getLog(CompositeRowKeyExpressionFactory.class);

  public CompositeRowKeyExpressionFactory(RowOperation graphRow) {
    super(graphRow);
  }

  public CompositeRowKeyExpressionFactory(PlasmaType rootType, StoreMappingContext mappingContext) {
    super(rootType, mappingContext);
  }

  @Override
  public String createRowKeyExpr(List<KeyValue> values) {
    StringBuilder result = new StringBuilder();

    if (values == null || values.size() == 0)
      throw new IllegalArgumentException(
          "expected non-null, non-zero length list argument 'values'");

    String keyValue = null;
    int i = 0;
    for (KeyFieldMapping fieldConfig : this.getGraph().getRowKeyFields()) {
      if (i > 0)
        this.buf.put(this.getGraph().getRowKeyFieldDelimiterBytes());
      if (fieldConfig instanceof MetaKeyFieldMapping) {
        MetaKeyFieldMapping predefinedConfig = (MetaKeyFieldMapping) fieldConfig;
        keyValue = new String(predefinedConfig.getKeyBytes(this.getRootType()), this.charset);
      } else if (fieldConfig instanceof DataRowKeyFieldMapping) {
        DataRowKeyFieldMapping userFieldConfig = (DataRowKeyFieldMapping) fieldConfig;
        KeyValue found = findTokenValue(userFieldConfig.getPropertyPath(), values);
        // user has a configuration for this path
        if (found != null) {
          keyValue = String.valueOf(found.getValue());
          switch (userFieldConfig.getCodecType()) {
          case HASH:
            if (found.isWildcard())
              throw new GraphKeyException(
                  "cannot create wildcard expression for user"
                      + " defined row-key field with XPath expression '"
                      + userFieldConfig.getPathExpression()
                      + "'"
                      + " for table '"
                      + this.getTable().getQualifiedPhysicalName()
                      + "' - this field is defined as using an integral hash algorithm which prevents the use of wildcards");
            break;
          default:
            if (found.isWildcard()) {
              String expr = getDataFlavorRegex(found.getProp().getDataFlavor());
              String replaceExpr = "\\" + found.getWildcard();
              keyValue = keyValue.replaceAll(replaceExpr, expr);
            }
            break;
          }
        } else {
          switch (userFieldConfig.getCodecType()) {
          case HASH:
            throw new GraphKeyException(
                "cannot default datatype expression for user"
                    + " defined row-key field with XPath expression '"
                    + userFieldConfig.getPathExpression()
                    + "'"
                    + " for table '"
                    + this.getTable().getQualifiedPhysicalName()
                    + "' - this field is defined as using an integral hash algorithm which prevents the use of wildcards");
          default:
            PlasmaProperty prop = (PlasmaProperty) userFieldConfig.getEndpointProperty();
            keyValue = getDataFlavorRegex(prop.getDataFlavor());
            break;
          }
        }
      }

      // if (fieldConfig.isHash()) {
      // keyValue = this.hashing.toString(keyValue);
      // }

      result.append(keyValue);

      i++;
    }

    return result.toString();
  }

  @Override
  public byte[] createRowKeyExprBytes(List<KeyValue> values) {

    if (values == null || values.size() == 0)
      throw new IllegalArgumentException(
          "expected non-null, non-zero length list argument 'values'");

    this.buf.clear();

    byte[] keyValue = null;
    int i = 0;
    for (KeyFieldMapping fieldConfig : this.getGraph().getRowKeyFields()) {
      if (i > 0)
        this.buf.put(this.getGraph().getRowKeyFieldDelimiterBytes());
      if (fieldConfig instanceof MetaKeyFieldMapping) {
        MetaKeyFieldMapping predefinedConfig = (MetaKeyFieldMapping) fieldConfig;
        keyValue = predefinedConfig.getKeyBytes(this.getRootType());
      } else if (fieldConfig instanceof DataRowKeyFieldMapping) {
        DataRowKeyFieldMapping userFieldConfig = (DataRowKeyFieldMapping) fieldConfig;
        KeyValue found = findTokenValue(userFieldConfig.getPropertyPath(), values);
        // user has a configuration for this path
        if (found != null) {
          String keyValueString = String.valueOf(found.getValue());
          switch (userFieldConfig.getCodecType()) {
          case HASH:
            if (found.isWildcard())
              throw new GraphKeyException(
                  "cannot create wildcard expression for user"
                      + " defined row-key field with XPath expression '"
                      + userFieldConfig.getPathExpression()
                      + "'"
                      + " for table '"
                      + this.getTable().getQualifiedPhysicalName()
                      + "' - this field is defined as using an integral hash algorithm which prevents the use of wildcards");
            break;
          default:
            if (found.isWildcard()) {
              String expr = getDataFlavorRegex(found.getProp().getDataFlavor());
              String replaceExpr = "\\" + found.getWildcard();
              keyValueString = keyValueString.replaceAll(replaceExpr, expr);
            }
            break;
          }
          keyValue = keyValueString.getBytes(charset);
        } else {
          switch (userFieldConfig.getCodecType()) {
          case HASH:
            throw new GraphKeyException(
                "cannot default datatype expression for user"
                    + " defined row-key field with XPath expression '"
                    + userFieldConfig.getPathExpression()
                    + "'"
                    + " for table '"
                    + this.getTable().getQualifiedPhysicalName()
                    + "' - this field is defined as using an integral hash algorithm which prevents the use of wildcards");
          default:
            PlasmaProperty prop = (PlasmaProperty) userFieldConfig.getEndpointProperty();
            keyValue = getDataFlavorRegex(prop.getDataFlavor()).getBytes(charset);
            break;
          }
        }
      }

      // if (fieldConfig.isHash()) {
      // keyValue = this.hashing.toStringBytes(keyValue);
      // }

      buf.put(keyValue);

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

  private KeyValue findTokenValue(String path, List<KeyValue> values) {
    for (KeyValue pair : values) {
      if (pair.getPropertyPath().equals(path))
        return pair;
    }
    return null;
  }

  private String getDataFlavorRegex(DataFlavor dataFlavor) {
    switch (dataFlavor) {
    case integral:
      return "[0-9\\-]+?";
    case real:
      return "[0-9\\-\\.]+?";
    default:
      return ".*?"; // any character zero or more times
    }
  }

  /**
   * Returns true if the data graph configured for the given
   * {@link commonj.sdo.Type type} has a user defined token which maps to the
   * given property path.
   * 
   * @param type
   *          the SDO type
   * @param path
   *          the property path
   * @return true if the data graph configured for the given
   *         {@link commonj.sdo.Type type} has a user defined token which maps
   *         to the given property path.
   */
  @Override
  public boolean hasUserDefinedRowKeyToken(Type type, String path) {
    return this.getGraph().getUserDefinedRowKeyField(path) != null;
  }

}
