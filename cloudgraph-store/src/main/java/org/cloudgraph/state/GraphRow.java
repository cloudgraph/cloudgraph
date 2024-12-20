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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.xml.namespace.QName;

import org.cloudgraph.common.Bytes;
import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataObject;

/**
 * Encapsulates the configuration and state related context information for a
 * specific table row including the management state for the underlying data
 * graph and the composite column key factory used to generated column keys
 * specific to a configured graph.
 * 
 * @see org.cloudgraph.store.mapping.DataGraphMapping
 * @author Scott Cinnamond
 * @since 0.5.1
 * 
 * @see GraphStatefullColumnKeyFactory
 */
public class GraphRow implements RowState {

  protected byte[] rowKey;
  protected SequenceGenerator sequenceMapping;
  protected DataGraphMapping graphConfig;
  protected GraphStatefullColumnKeyFactory columnKeyFactory;
  protected DataObject rootDataObject;
  private Map<Integer, DataObject> dataObjectMap = new HashMap<>();
  public static final String ROOT_TYPE_DELIM = "#";

  @SuppressWarnings("unused")
  private GraphRow() {
  }

  public GraphRow(byte[] rowKey, DataObject rootDataObject, StoreMappingContext mappingContext) {
    this.rowKey = rowKey;
    this.rootDataObject = rootDataObject;
    int hashCode = getHashCode((PlasmaDataObject) this.rootDataObject);
    this.dataObjectMap.put(hashCode, rootDataObject);

    QName rootTypeQname = ((PlasmaType) this.rootDataObject.getType()).getQualifiedName();
    this.graphConfig = StoreMapping.getInstance().getDataGraph(rootTypeQname, mappingContext);
  }

  @Override
  public byte[] getRowKey() {
    return this.rowKey;
  }

  @Override
  public DataGraphMapping getDataGraph() {
    return this.graphConfig;
  }

  public SequenceGenerator getSequenceMapping() throws IOException {
    return sequenceMapping;
  }

  public GraphStatefullColumnKeyFactory getColumnKeyFactory() throws IOException {
    return columnKeyFactory;
  }

  /**
   * Returns the root data object associated with the row operation.
   * 
   * @return the root data object associated with the row operation.
   */
  @Override
  public DataObject getRootDataObject() {
    return this.rootDataObject;
  }

  /**
   * Returns the root type associated with the row operation.
   * 
   * @return the root type associated with the row operation.
   */
  public PlasmaType getRootType() {
    return (PlasmaType) this.rootDataObject.getType();
  }

  /**
   * Adds the given data object as associated with the row operation.
   * 
   * @param dataObject
   *          the root data object
   * @throws IllegalArgumentException
   *           if the given data object is already mapped
   */
  @Override
  public void addDataObject(DataObject dataObject) {
    int hashCode = getHashCode((PlasmaDataObject) dataObject);
    if (this.dataObjectMap.get(hashCode) != null) {
      throw new IllegalArgumentException("data object already added, " + dataObject);
    }
    this.dataObjectMap.put(hashCode, dataObject);
  }

  /**
   * Returns true if this row operation is associated with the given data
   * object.
   * 
   * @return true if this row operation is associated with the given data
   *         object.
   */
  @Override
  public boolean contains(DataObject dataObject) {
    int hashCode = getHashCode((PlasmaDataObject) dataObject);
    return this.dataObjectMap.containsKey(hashCode);
  }

  /**
   * Returns true if this row operation is associated with the given data object
   * UUID .
   * 
   * @return true if this row operation is associated with the given data object
   *         UUID .
   */
  public boolean contains(java.util.UUID uuid) {
    int hashCode = getHashCode(uuid);
    return this.dataObjectMap.containsKey(hashCode);
  }

  /**
   * Returns the data object associated with this row operation based on the
   * given data object UUID .
   * 
   * @return the data object associated with this row operation based on the
   *         given data object UUID .
   */
  public DataObject getDataObject(java.util.UUID uuid) {
    int hashCode = getHashCode(uuid);
    DataObject result = this.dataObjectMap.get(hashCode);
    if (result == null)
      throw new IllegalArgumentException("data object (" + uuid + ") not found");
    return result;
  }

  @Override
  public boolean contains(Long sequence, PlasmaType type) {
    int hashCode = getHashCode(sequence, type);
    return this.dataObjectMap.containsKey(hashCode);
  }

  @Override
  public DataObject getDataObject(Long sequence, PlasmaType type) {
    int hashCode = getHashCode(sequence, type);
    DataObject result = this.dataObjectMap.get(hashCode);
    if (result == null)
      throw new IllegalArgumentException("data object (" + sequence
          + ") not found for sequence/type, " + String.valueOf(sequence) + "/" + type);
    return result;
  }

  @Override
  public void addDataObject(DataObject dataObject, Long sequence, PlasmaType type) {
    int hashCode = getHashCode(sequence, type);
    DataObject result = this.dataObjectMap.get(hashCode);
    if (result != null)
      throw new IllegalArgumentException("data object (" + sequence
          + ") already exists for sequence/type, " + String.valueOf(sequence) + "/" + type);
    this.dataObjectMap.put(hashCode, dataObject);
  }

  @Override
  public boolean contains(DataObject dataObject, Long sequence, PlasmaType type) {
    int hashCode = getHashCode(sequence, type);
    return this.dataObjectMap.containsKey(hashCode);
  }

  protected int getHashCode(UUID uuid) {
    return uuid.hashCode();
  }

  protected int getHashCode(PlasmaDataObject dataObject) {
    return dataObject.getUUID().hashCode();
  }

  protected int getHashCode(PlasmaType type, PlasmaProperty property, Long sequence) {
    int result = 1;
    result = 31 * result + type.getQualifiedNameHashCode();
    result = 31 * result + property.getName().hashCode();
    result = 31 * result + sequence.hashCode();
    return result;
  }

  protected int getHashCode(PlasmaDataObject dataObject, PlasmaProperty property) {
    int result = 1;
    result = 31 * result + getHashCode(dataObject);
    result = 31 * result + property.getName().hashCode();
    return result;
  }

  protected int getHashCode(PlasmaType type, PlasmaProperty property) {
    if (type == null)
      throw new IllegalArgumentException("expected arg type");
    if (property == null)
      throw new IllegalArgumentException("expected arg property");
    int result = 1;
    result = 31 * result + type.getQualifiedNameHashCode();
    result = 31 * result + property.getName().hashCode();
    return result;
  }

  public static int getHashCode(Long sequence, PlasmaType type) {
    int result = 1;
    result = 31 * result + sequence.hashCode();
    result = 31 * result + type.getQualifiedNameHashCode();
    return result;
  }

}
