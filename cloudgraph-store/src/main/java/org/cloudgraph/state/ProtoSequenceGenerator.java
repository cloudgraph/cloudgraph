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
package org.cloudgraph.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.state.proto.StateModelProto;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreConstants;

import commonj.sdo.DataObject;
import commonj.sdo.Type;

public class ProtoSequenceGenerator implements SequenceGenerator {
  private static Log log = LogFactory.getLog(ProtoSequenceGenerator.class);
  public static final Charset charset = Charset.forName(CoreConstants.UTF8_ENCODING);
  private boolean updated = false;
  private boolean marshaled = false;

  private StateModelProto.StateModel.Builder model;

  /** Maps URI strings to URI state structures */
  private Map<String, StateModelProto.URI.Builder> uriMap = new HashMap<>();
  /** Maps qualified names to types */
  private Map<Integer, StateModelProto.TypeEntry.Builder> typeNameMap = new HashMap<>();

  public ProtoSequenceGenerator() {
    this.model = StateModelProto.StateModel.newBuilder();
  }

  public ProtoSequenceGenerator(byte[] state) {

    if (state == null)
      throw new IllegalArgumentException("expected arg, state");
    this.model = this.fromBytes(state);

    for (StateModelProto.URI.Builder uri : model.getUriBuilderList()) {
      if (this.uriMap.containsKey(uri.getName()))
        throw new StateException("mulitple URI's mapped to name '" + uri.getName() + "'");
      this.uriMap.put(uri.getName(), uri);
      for (StateModelProto.TypeEntry.Builder type : uri.getTypeEntryBuilderList()) {
        type.setUriName(uri.getName());
        int typeHash = this.getTypeHashCode(uri.getName(), type.getName());
        if (this.typeNameMap.containsKey(typeHash))
          throw new StateException("mulitple types mapped to uri/type '" + uri.getName() + "/"
              + type.getName() + "'");
        this.typeNameMap.put(typeHash, type);
      }
    }
    // now that all element builders are mapped, clear the
    // collections, as builders are immutable, so
    // we only connect them before marshaling
    for (StateModelProto.URI.Builder uri : model.getUriBuilderList())
      uri.clearTypeEntry();

    model.clearUri();

  }

  private int getTypeHashCode(PlasmaType type) {
    return getTypeHashCode(getUriName(type), getTypeName(type));
  }

  private int getTypeHashCode(String uriName, String typeName) {
    int result = 1;
    result = 31 * result + uriName.hashCode();
    result = 31 * typeName.hashCode();
    return result;
  }

  private String getUriName(PlasmaType type) {
    String name = type.getURIPhysicalName();
    if (name == null) {
      log.warn("no URI physical name available for type, "
          + type
          + ", encoding logical URI name - please annotate your model with physical name aliases to facilitate logical/physical name isolation");
      name = type.getURI();
    }
    return name;
  }

  private String getTypeName(PlasmaType type) {
    String name = type.getPhysicalName();
    if (name == null) {
      log.warn("no physical name available for type, "
          + type
          + ", encoding logical name - please annotate your model with physical name aliases to facilitate logical/physical name isolation");
      name = type.getName();
    }
    return name;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean hasLastSequence(PlasmaType type) {
    return this.typeNameMap.containsKey(getTypeHashCode(type));
  }

  @Override
  public Long lastSequence(PlasmaType type) {
    StateModelProto.TypeEntry.Builder typeEntry = this.typeNameMap.get(getTypeHashCode(type));
    return typeEntry.getSequence();
  }

  @Override
  public Long nextSequence(DataObject dataObject) {
    PlasmaType type = (PlasmaType) dataObject.getType();
    return nextSequence(type);
  }

  @Override
  public Long nextSequence(PlasmaType type) {
    if (this.marshaled == true)
      throw new IllegalStateException("protocol has already been marshalled, and cannot be updated");
    String uriName = getUriName(type);
    String typeName = getTypeName(type);
    int typeHash = this.getTypeHashCode(uriName, typeName);
    StateModelProto.TypeEntry.Builder typeEntry = this.typeNameMap.get(typeHash);
    if (typeEntry == null) { // type not mapped
      typeEntry = StateModelProto.TypeEntry.newBuilder();
      typeEntry.setName(typeName);
      typeEntry.setSequence(1);
      typeEntry.setHashValue(type.getQualifiedNameHashCode());
      if (log.isDebugEnabled())
        log.debug("adding type " + type.getQualifiedName() + " seq: " + typeEntry.getSequence()
            + " hash: " + typeEntry.getHashValue());
      this.typeNameMap.put(typeHash, typeEntry);

      StateModelProto.URI.Builder uri = this.uriMap.get(uriName);
      if (uri == null) { // uri not mapped
        uri = StateModelProto.URI.newBuilder();
        uri.setName(uriName);
        this.uriMap.put(uriName, uri);
        // connect builders only on marshal
      }
      typeEntry.setUriName(uri.getName());
      // connect builders only on marshal
    } else {
      typeEntry.setSequence(typeEntry.getSequence() + 1);
    }

    this.updated = true;
    return typeEntry.getSequence();
  }

  @Override
  public String marshalAsString() {
    return new String(marshal(), this.charset);
  }

  @Override
  public String marshalAsString(boolean formatted) {
    return marshalAsString();
  }

  /**
   * Connects builder elements "depth first" as the proto bjuilders are
   * immutable. Note: This method can only be called once.
   */
  @Override
  public byte[] marshal() {
    for (StateModelProto.TypeEntry.Builder type : this.typeNameMap.values()) {
      StateModelProto.URI.Builder uri = this.uriMap.get(type.getUriName());
      uri.addTypeEntry(type);
    }
    for (StateModelProto.URI.Builder uri : this.uriMap.values()) {
      this.model.addUri(uri);
    }

    StateModelProto.StateModel result = this.model.build();
    if (log.isDebugEnabled())
      log.debug("marshal: " + result);

    this.marshaled = true;
    return toBytes(result);
  }

  private byte[] toBytes(StateModelProto.StateModel model) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] bytes = null;
    try {
      model.writeDelimitedTo(baos);
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

  private StateModelProto.StateModel.Builder fromBytes(byte[] content) {
    ByteArrayInputStream bais = new ByteArrayInputStream(content);
    try {
      StateModelProto.StateModel.Builder result = StateModelProto.StateModel.newBuilder();
      result.mergeDelimitedFrom(bais);
      return result;
    } catch (IOException e) {
      throw new StateException(e);
    } finally {
      try {
        bais.close();
      } catch (IOException e) {
      }
    }
  }

  public String toString() {
    return this.model.toString();
  }

  @Override
  public boolean isUpdated() {
    return this.updated;
  }
}
