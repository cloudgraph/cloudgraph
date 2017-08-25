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

import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.config.CloudGraphConfigurationException;
import org.cloudgraph.config.PreDefinedKeyFieldConfig;
import org.cloudgraph.config.PredefinedField;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.scan.StringLiteral;
import org.cloudgraph.hbase.service.CloudGraphContext;
import org.cloudgraph.store.key.KeyValue;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;

/**
 * Delegate class supporting composite key generation.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class KeySupport {
  private static final Log log = LogFactory.getLog(CompositeRowKeyFactory.class);

  /**
   * Returns the specific configured hash algorithm configured for an HTable, or
   * if not configured returns the configured HBase hash algorithm as configured
   * within HBase using the 'hbase.hash.type' property.
   * 
   * @return the specific configured hash algorithm configured for an HTable.
   */
  public Hash getHashAlgorithm(TableConfig table) {
    Hash hash = null;
    if (table.hasHashAlgorithm()) {
      String hashName = table.getTable().getHashAlgorithm().getName().value();
      hash = Hash.getInstance(Hash.parseHashType(hashName));
    } else {
      String algorithm = CloudGraphContext.instance().getConfig()
          .get(CloudGraphConstants.PROPERTY_CONFIG_HASH_TYPE);
      if (algorithm != null)
        hash = Hash.getInstance(Hash.parseHashType(algorithm));
    }
    return hash;
  }

  public KeyValue findKeyValue(UserDefinedRowKeyFieldConfig fieldConfig, List<KeyValue> pairs) {

    PlasmaProperty fieldProperty = fieldConfig.getEndpointProperty();

    for (KeyValue keyValue : pairs) {
      if (keyValue.getProp().equals(fieldProperty)) {
        if (fieldConfig.getPropertyPath() != null && keyValue.getPropertyPath() != null) {
          if (keyValue.getPropertyPath().equals(fieldConfig.getPropertyPath())) {
            return keyValue;
          }
        }
      }
    }
    return null;
  }

  /**
   * Returns a token value from the given Type
   * 
   * @param type
   *          the SDO Type
   * @param hash
   *          the hash algorithm to use in the event the row key token is to be
   *          hashed
   * @param token
   *          the pre-defined row key token configuration
   * @return the token value
   */
  public String getPredefinedFieldValue(PlasmaType type, Hashing hashing,
      PreDefinedKeyFieldConfig token) {
    String result = null;
    switch (token.getName()) {
    case URI:
      result = type.getURIPhysicalName();
      if (result == null || result.length() == 0) {
        if (log.isDebugEnabled())
          log.debug("no URI physical name for type, " + type + ", defined - using logical name");
        result = type.getURI();
      }
      break;
    case TYPE:
      result = type.getPhysicalName();
      if (result == null || result.length() == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical name for type, " + type + ", defined - using logical name");
        result = type.getName();
      }
      break;
    default:
      throw new CloudGraphConfigurationException("invalid row key token name, "
          + token.getName().name() + " - cannot get this token from a SDO Type");
    }

    if (token.isHash()) {
      result = hashing.toString(result);
    }

    return result;
  }

  public byte[] getPredefinedFieldValueStartBytes(PlasmaType type, Hashing hashing,
      PreDefinedKeyFieldConfig token) {
    return getPredefinedFieldValueBytes(type, hashing, token);
  }

  public byte[] getPredefinedFieldValueStopBytes(PlasmaType type, Hashing hashing,
      PreDefinedKeyFieldConfig token) {
    byte[] result = null;
    switch (token.getName()) {
    case URI:
      if (token.isHash()) {
        byte[] uriBytes = type.getURIPhysicalNameBytes();
        if (uriBytes == null || uriBytes.length == 0) {
          if (log.isDebugEnabled())
            log.debug("no URI physical name for type, " + type + ", defined - using logical name");
          uriBytes = type.getURIBytes();
        }
        result = hashing.toStringBytes(uriBytes, 1);
      } else {
        String uriName = type.getURIPhysicalName();
        if (uriName == null || uriName.length() == 0) {
          if (log.isDebugEnabled())
            log.debug("no URI physical name for type, " + type + ", defined - using logical name");
          uriName = type.getURI();
        }
        uriName += StringLiteral.INCREMENT;
        result = Bytes.toBytes(uriName);
      }
      break;
    case TYPE:
      if (token.isHash()) {
        byte[] nameBytes = type.getPhysicalNameBytes();
        if (nameBytes == null || nameBytes.length == 0) {
          if (log.isDebugEnabled())
            log.debug("no physical name for type, " + type + ", defined - using logical name");
          nameBytes = type.getNameBytes();
        }
        result = hashing.toStringBytes(nameBytes, 1);
      } else {
        String nameString = type.getPhysicalName();
        if (nameString == null) {
          if (log.isDebugEnabled())
            log.debug("no physical name for type, " + type + ", defined - using logical name");
          nameString = type.getName();
        }
        nameString += StringLiteral.INCREMENT;
        result = Bytes.toBytes(nameString);
      }
      break;
    default:
      throw new CloudGraphConfigurationException("invalid row key token name, "
          + token.getName().name() + " - cannot get this token from a SDO Type");
    }

    return result;
  }

  public byte[] getPredefinedFieldValueBytes(PlasmaType type, Hashing hashing,
      PreDefinedKeyFieldConfig token) {
    byte[] result = null;
    switch (token.getName()) {
    case URI:
      result = type.getURIPhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no URI physical name for type, " + type + ", defined - using logical name");
        result = type.getURIBytes();
      }
      break;
    case TYPE:
      result = type.getPhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical name for type, " + type + ", defined - using logical name");
        result = type.getNameBytes();
      }
      break;
    default:
      throw new CloudGraphConfigurationException("invalid row key token name, "
          + token.getName().name() + " - cannot get this token from a SDO Type");
    }

    if (token.isHash()) {
      result = hashing.toStringBytes(result);
    }

    return result;
  }

  /**
   * Returns a token value from the given Data Graph
   * 
   * @param dataGraph
   *          the data graph
   * @param hash
   *          the hash algorithm to use in the event the row key token is to be
   *          hashed
   * @param token
   *          the pre-defined row key token configuration
   * @return the token value
   */
  public byte[] getPredefinedFieldValueBytes(DataGraph dataGraph, Hash hash, PredefinedField token) {
    return getPredefinedFieldValueBytes(dataGraph.getRootObject(), hash, token);
  }

  /**
   * Returns a token value from the given data object
   * 
   * @param dataObject
   *          the root data object
   * @param hash
   *          the hash algorithm to use in the event the row key token is to be
   *          hashed
   * @param token
   *          the pre-defined row key token configuration
   * @return the token value
   */
  public byte[] getPredefinedFieldValueBytes(DataObject dataObject, Hash hash, PredefinedField token) {
    PlasmaType rootType = (PlasmaType) dataObject.getType();

    byte[] result = null;
    switch (token.getName()) {
    case URI:
      result = rootType.getURIBytes();
      break;
    case TYPE:
      QName qname = rootType.getQualifiedName();

      result = rootType.getPhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical name for type, " + qname.getNamespaceURI() + "#"
              + rootType.getName() + ", defined - using logical name");
        result = rootType.getNameBytes();
      }
      break;
    case UUID:
      result = Bytes.toBytes(((PlasmaDataObject) dataObject).getUUIDAsString());
      break;
    default:
      throw new CloudGraphConfigurationException("invalid row key token name, "
          + token.getName().name() + " - cannot get this token from a Data Graph");
    }

    if (token.isHash()) {
      int hashValue = hash.hash(result);
      result = Bytes.toBytes(String.valueOf(hashValue));
    }

    return result;
  }
}
