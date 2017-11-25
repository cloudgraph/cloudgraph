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
package org.cloudgraph.store.mapping;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.mapping.PreDefinedFieldName;
import org.cloudgraph.store.mapping.PredefinedField;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;

/**
 * Encapsulates logic related to access of a configured pre-defined row or
 * column key field.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class PreDefinedKeyFieldMapping extends KeyFieldMapping {

  private static final Log log = LogFactory.getLog(PreDefinedKeyFieldMapping.class);
  private PredefinedField field;

  public PreDefinedKeyFieldMapping(PredefinedField field, int seqNum, int totalFields) {
    super(field, seqNum, totalFields);
    this.field = field;
  }

  public PredefinedField getField() {
    return field;
  }

  public PreDefinedFieldName getName() {
    return field.getName();
  }

  public boolean isHash() {
    return field.isHash();
  }

  @Override
  public String getKey(DataGraph dataGraph) {
    return getKey(dataGraph.getRootObject());
  }

  @Override
  public String getKey(DataObject dataObject) {
    PlasmaType rootType = (PlasmaType) dataObject.getType();

    String result = null;
    switch (this.getName()) {
    case URI:
      result = rootType.getURIPhysicalName();
      if (result == null || result.length() == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical URI name for type, " + rootType.toString()
              + ", defined - using URI");
        result = rootType.getURI();
      }
      break;
    case TYPE:
      result = rootType.getPhysicalName();
      if (result == null || result.length() == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical name for type, " + rootType.toString()
              + ", defined - using logical name");
        result = rootType.getName();
      }
      break;
    case PKG:
      result = rootType.getPackagePhysicalName();
      if (result == null || result.length() == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical package name for type, " + rootType.toString()
              + ", defined - using URI");
        result = rootType.getURI();
      }
      break;
    case UUID:
      result = ((PlasmaDataObject) dataObject).getUUIDAsString();
      break;
    default:
      throw new StoreMappingException("invalid row key field name, " + this.getName().name()
          + " - cannot get this field from a Data Graph");
    }

    return result;
  }

  /**
   * Returns a key value from the given Data Graph
   * 
   * @param dataGraph
   *          the data graph
   * @return the key value
   */
  public byte[] getKeyBytes(commonj.sdo.DataGraph dataGraph) {
    return this.getKeyBytes(dataGraph.getRootObject());
  }

  /**
   * Returns a key value from the given data object
   * 
   * @param dataObject
   *          the root data object
   * @return the key value
   */
  public byte[] getKeyBytes(DataObject dataObject) {
    PlasmaType rootType = (PlasmaType) dataObject.getType();

    byte[] result = null;
    switch (this.getName()) {
    case URI:
      result = rootType.getURIPhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical URI name bytes for type, " + rootType.toString()
              + ", defined - using URI bytes");
        result = rootType.getURIBytes();
      }
      break;
    case TYPE:
      result = rootType.getPhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical name bytes for type, " + rootType.toString()
              + ", defined - using logical name bytes");
        result = rootType.getNameBytes();
      }
      break;
    case PKG:
      result = rootType.getPackagePhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical package name bytes for type, " + rootType.toString()
              + ", defined - using URI bytes");
        result = rootType.getURIBytes();
      }
      break;
    case UUID:
      result = ((PlasmaDataObject) dataObject).getUUIDAsString().getBytes(charset);
      break;
    default:
      throw new StoreMappingException("invalid row key field name, " + this.getName().name()
          + " - cannot get this field from a Data Graph");
    }

    return result;
  }

  /**
   * Returns a key value from the given type
   * 
   * @param type
   *          the type
   * @return the key value
   */
  public byte[] getKeyBytes(PlasmaType type, PlasmaProperty property) {
    switch (this.getName()) {
    case URI:
    case TYPE:
    case PKG:
      return getKeyBytes(type);
    case PROPERTY:
      return getKeyBytes(property);
    default:
      throw new StoreMappingException("invalid predefined key field name, " + this.getName().name()
          + " - cannot get this predefined field from a type or property");
    }
  }

  public byte[] getKeyBytes(PlasmaType type, EntityMetaKey metaField) {
    switch (this.getName()) {
    case URI:
    case TYPE:
    case PKG:
      return getKeyBytes(type);
    case PROPERTY:
      return getKeyBytes(metaField);
    default:
      throw new StoreMappingException("invalid predefined key field name, " + this.getName().name()
          + " - cannot get this predefined field from a type or property");
    }
  }

  public byte[] getKeyBytes(PlasmaType type, EdgeMetaKey metaField) {
    switch (this.getName()) {
    case URI:
    case TYPE:
    case PKG:
      return getKeyBytes(type);
    case PROPERTY:
      return getKeyBytes(metaField);
    default:
      throw new StoreMappingException("invalid predefined key field name, " + this.getName().name()
          + " - cannot get this predefined field from a type or property");
    }
  }

  /**
   * Returns a key value from the given type
   * 
   * @param type
   *          the type
   * @return the key value
   */
  public byte[] getKeyBytes(PlasmaType type) {
    byte[] result = null;
    switch (this.getName()) {
    case URI:
      result = type.getURIPhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical URI name bytes for type, " + type.toString()
              + ", defined - using URI bytes");
        result = type.getURIBytes();
      }
      break;
    case TYPE:
      result = type.getPhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical name for type, " + type.toString()
              + ", defined - using logical name");
        result = type.getNameBytes();
      }
      break;
    case PKG:
      result = type.getPackagePhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical package name bytes for type, " + type.toString()
              + ", defined - using URI bytes");
        result = type.getURIBytes();
      }
      break;
    default:
      throw new StoreMappingException("invalid predefined key field name, " + this.getName().name()
          + " - cannot get this predefined field from a type");
    }

    return result;
  }

  /**
   * Returns a key value from the given type
   * 
   * @param type
   *          the type
   * @return the key value
   */
  public byte[] getKeyBytes(PlasmaProperty property) {
    byte[] result = null;
    switch (this.getName()) {
    case PROPERTY:
      result = property.getPhysicalNameBytes();
      if (result == null || result.length == 0) {
        if (log.isDebugEnabled())
          log.debug("no physical name for property, " + property.toString()
              + ", defined - using logical name");
        result = property.getNameBytes();
      }
      break;
    default:
      throw new StoreMappingException("invalid predefined key field name, " + this.getName().name()
          + " - cannot get this predefined field from a property");
    }

    return result;
  }

  public byte[] getKeyBytes(EntityMetaKey metaField) {
    byte[] result = null;
    switch (this.getName()) {
    case PROPERTY:
      result = metaField.codeAsBytes();
      break;
    default:
      throw new StoreMappingException("invalid predefined key field name, " + this.getName().name()
          + " - cannot get this predefined field from a metaField");
    }

    return result;
  }

  public byte[] getKeyBytes(EdgeMetaKey metaField) {
    byte[] result = null;
    switch (this.getName()) {
    case PROPERTY:
      result = metaField.codeAsBytes();
      break;
    default:
      throw new StoreMappingException("invalid predefined key field name, " + this.getName().name()
          + " - cannot get this predefined field from a metaField");
    }

    return result;
  }

  // FIXME: drive these from
  // global configuration settings
  @Override
  public int getMaxLength() {
    switch (this.getName()) {
    case URI:
      return 12;
    case TYPE:
      return 32;
    case UUID:
      return 36;
    default:
      return 12;
    }
  }

  public DataFlavor getDataFlavor() {
    switch (this.getName()) {
    case URI:
      return DataFlavor.string;
    case TYPE:
      return DataFlavor.string;
    case UUID:
      return DataFlavor.string;
    default:
      return DataFlavor.string;
    }
  }

}
