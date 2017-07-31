/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.state;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreConstants;

import commonj.sdo.DataObject;

/**
 * Stores sequence generator state information for a graph row using JAXB data
 * binding and validation.
 * <p>
 * Every property of every entity regardless of whether it is part of a
 * collection must contain a sequence number that is unique for the entity type
 * within the local graph row. This is essential as a single graph may contain
 * multiple collections of the same entity type. The current sequence number for
 * each entity type is maintained as state information for the graph row. This
 * is minimal and consists of a simple mapping of type physical names, mapped to
 * long integers. At runtime this state information is used within a sequence
 * generator, with changes being written back to the graph row on commit.
 * </p>
 * 
 * @see org.cloudgraph.state.StateModel
 * @see org.cloudgraph.state.URI
 * @see org.cloudgraph.state.TypeEntry
 * 
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class BindingSequenceGenerator implements SequenceGenerator {

  private static Log log = LogFactory.getLog(BindingSequenceGenerator.class);
  public static final Charset charset = Charset.forName(CoreConstants.UTF8_ENCODING);

  private StateModel model = null;
  private StateMarshalingContext context;

  /** The UUID which identified the root data object within a graph */
  // private java.util.UUID rootUUID;

  /** Maps URI strings to URI state structures */
  private Map<String, URI> uriMap = new HashMap<String, URI>();
  /** Maps qualified names to types */
  private Map<Integer, TypeEntry> typeNameMap = new HashMap<>();
  private boolean updated = false;

  @SuppressWarnings("unused")
  private BindingSequenceGenerator() {
  }

  public BindingSequenceGenerator(StateMarshalingContext context) {
    // this.rootUUID = rootUUID;
    this.context = context;
    // if (this.rootUUID == null)
    // throw new IllegalArgumentException("expected arg, rootUUID");
    if (this.context == null)
      throw new IllegalArgumentException("expected arg, context");
    this.model = new StateModel();
    // Root root = new Root();
    // root.setValue(this.rootUUID.toString());
    // this.model.setRoot(root);
  }

  public BindingSequenceGenerator(String state, StateMarshalingContext context) {

    this.context = context;
    if (state == null)
      throw new IllegalArgumentException("expected arg, state");
    if (this.context == null)
      throw new IllegalArgumentException("expected arg, context");

    if (log.isDebugEnabled())
      log.debug("unmarshal raw: " + state);
    NonValidatingDataBinding binding = this.context.getBinding();
    try {
      model = (StateModel) binding.unmarshal(state);
    } catch (JAXBException e) {
      throw new StateException(e);
    } finally {
      if (binding != null)
        this.context.returnBinding(binding);
    }

    // try {
    // this.rootUUID = java.util.UUID.fromString(model.getRoot().getValue());
    // }
    // catch (NullPointerException npe) {
    // throw new StateException("no root found for sequence mapping");
    // }

    for (URI uri : model.getURIS()) {
      if (this.uriMap.containsKey(uri.getName()))
        throw new StateException("mulitple URI's mapped to name '" + uri.getName() + "'");
      this.uriMap.put(uri.getName(), uri);
      for (TypeEntry type : uri.getTypes()) {
        type.setUriName(uri.getName());
        int typeHash = this.getTypeHashCode(uri.getName(), type.getName());
        if (this.typeNameMap.containsKey(typeHash))
          throw new StateException("mulitple types mapped to uri/type '" + uri.getName() + "/"
              + type.getName() + "'");
        this.typeNameMap.put(typeHash, type);
      }
    }

    if (log.isDebugEnabled())
      validate();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.FOO#getRootUUID()
   */
  // @Override
  // public java.util.UUID getRootUUID() {
  // return rootUUID;
  // }

  private void validate() {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.FOO#close()
   */
  @Override
  public void close() {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.FOO#hasLastSequence(org.plasma.sdo.PlasmaType)
   */
  @Override
  public boolean hasLastSequence(PlasmaType type) {
    TypeEntry typeEntry = this.typeNameMap.get(type.getQualifiedName());
    return typeEntry != null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.FOO#lastSequence(org.plasma.sdo.PlasmaType)
   */
  @Override
  public Long lastSequence(PlasmaType type) {
    TypeEntry typeEntry = this.typeNameMap.get(type.getQualifiedName());
    return typeEntry.getSeq();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.FOO#nextSequence(commonj.sdo.DataObject)
   */
  @Override
  public Long nextSequence(DataObject dataObject) {
    PlasmaType type = (PlasmaType) dataObject.getType();
    return nextSequence(type);
  }

  private int getTypeHashCode(String uriName, String typeName) {
    int result = 1;
    result = 31 * result + uriName.hashCode();
    result = 31 * typeName.hashCode();
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.FOO#nextSequence(org.plasma.sdo.PlasmaType)
   */
  @Override
  public Long nextSequence(PlasmaType type) {
    String uriName = type.getURIPhysicalName();
    if (uriName == null) {
      log.warn("no URI physical name available for type, "
          + type
          + ", encoding logical URI name - please annotate your model with physical name aliases to facilitate logical/physical name isolation");
      uriName = type.getURI();
    }
    String typeName = type.getPhysicalName();
    if (typeName == null) {
      log.warn("no physical name available for type, "
          + type
          + ", encoding logical name - please annotate your model with physical name aliases to facilitate logical/physical name isolation");
      typeName = type.getName();
    }
    int typeHash = this.getTypeHashCode(uriName, typeName);
    TypeEntry typeEntry = this.typeNameMap.get(typeHash);
    if (typeEntry == null) { // type not mapped
      typeEntry = new TypeEntry();
      typeEntry.setName(typeName);
      typeEntry.setSeq(1);
      typeEntry.setHashCode(type.getQualifiedNameHashCode());
      if (log.isDebugEnabled())
        log.debug("adding type " + type.getQualifiedName() + " seq: " + typeEntry.getSeq()
            + " hash: " + typeEntry.getHashCode());
      this.typeNameMap.put(typeHash, typeEntry);

      URI uri = this.uriMap.get(uriName);
      if (uri == null) { // uri not mapped
        uri = new URI();
        uri.setName(uriName);
        this.uriMap.put(uriName, uri);
        this.model.getURIS().add(uri);
      }
      typeEntry.setUriName(uri.getName());
      uri.getTypes().add(typeEntry);
    } else {
      typeEntry.setSeq(typeEntry.getSeq() + 1);
    }

    if (log.isDebugEnabled())
      validate();

    this.updated = true;
    return typeEntry.getSeq();
  }

  @Override
  public String marshalAsString() {
    if (log.isDebugEnabled())
      validate();
    return marshalAsString(false);
  }

  @Override
  public byte[] marshal() {
    return marshalAsString().getBytes(charset);
  }

  @Override
  public String marshalAsString(boolean formatted) {
    NonValidatingDataBinding binding = null;
    String xml = "";
    try {

      // null out URI on individual types as uri
      // found on URI container
      // for (TypeEntry entry : this.typeNameMap.values()) {
      // entry.setUri(null);

      binding = this.context.getBinding();
      xml = binding.marshal(this.model); // no formatting
    } catch (JAXBException e1) {
      throw new StateException(e1);
    } finally {
      if (binding != null)
        this.context.returnBinding(binding);
    }

    return xml;
  }

  @Override
  public boolean isUpdated() {
    return this.updated;
  }

}
