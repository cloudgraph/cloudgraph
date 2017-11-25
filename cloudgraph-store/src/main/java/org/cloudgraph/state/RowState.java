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

import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataObject;

/**
 * Provides access to the configuration and state related context information
 * for a specific table row including the management state for the underlying
 * data graph and the composite column key factory used to generated column keys
 * specific to a configured graph.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface RowState {

  /**
   * Returns the composite row key bytes for this row context
   * 
   * @return the composite row key bytes for this row context
   */
  public byte[] getRowKey();

  public DataGraphMapping getDataGraph();

  /**
   * Returns the root data object associated with the row operation.
   * 
   * @return the root data object associated with the row operation.
   */
  public DataObject getRootDataObject();

  /**
   * Returns the root type associated with the row operation.
   * 
   * @return the root type associated with the row operation.
   */
  public PlasmaType getRootType();

  /**
   * Adds the given data object as associated with the row operation.
   * 
   * @param dataObject
   *          the root data object
   */
  public void addDataObject(DataObject dataObject);

  /**
   * Returns true if this row operation is associated with the given data
   * object.
   * 
   * @return true if this row operation is associated with the given data
   *         object.
   */
  public boolean contains(DataObject dataObject);

  /**
   * Returns true if this row operation is associated with the given data object
   * UUID .
   * 
   * @return true if this row operation is associated with the given data object
   *         UUID .
   */
  public boolean contains(java.util.UUID uuid);

  /**
   * Returns true if this row operation is associated with the given sequence
   * for the given base type.
   * 
   * @param sequence
   *          the sequence
   * @param type
   *          the type
   * @return true if this row operation is associated with the given sequence
   *         for the given base type.
   */
  public boolean contains(Long sequence, PlasmaType type);

  /**
   * Returns the data object associated with the given sequence for the given
   * base type.
   * 
   * @param sequence
   *          the sequence
   * @param type
   *          the type
   * @return the data object associated with the given sequence for the given
   *         base type.
   */
  public DataObject getDataObject(Long sequence, PlasmaType type);

  public boolean contains(DataObject dataObject, Long sequence, PlasmaType type);

  public void addDataObject(DataObject dataObject, Long sequence, PlasmaType type);

  /**
   * Returns the data object associated with this row operation based on the
   * given data object UUID .
   * 
   * @return the data object associated with this row operation based on the
   *         given data object UUID .
   */
  public DataObject getDataObject(java.util.UUID uuid);

  /**
   * Returns the sequence mapping for this row context
   * 
   * @return the sequence mapping for this row context
   */
  public SequenceGenerator getSequenceMapping() throws IOException;

  /**
   * Returns the column key factory for this row context
   * 
   * @return the column key factory for this row context
   */
  public GraphStatefullColumnKeyFactory getColumnKeyFactory() throws IOException;

}
