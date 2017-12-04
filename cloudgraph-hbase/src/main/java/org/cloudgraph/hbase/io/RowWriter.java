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
package org.cloudgraph.hbase.io;

import java.io.IOException;

import org.cloudgraph.hbase.key.StatefullColumnKeyFactory;
import org.cloudgraph.hbase.mutation.Mutations;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataObject;

/**
 * Provides access to the operational, configuration and other state information
 * required for write operations on a single graph row.
 * <p>
 * Acts as a single component within a {@link TableWriter} container and
 * encapsulates the HBase client <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html"
 * >Put</a> and <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Delete.html"
 * >Delete</a> operations for use in write operations across multiple logical
 * entities within a graph row.
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.TableWriter
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface RowWriter extends RowOperation {

  /**
   * Returns the row put mutation.
   * 
   * @return the row put mutation.
   */
  // public Put getPut();

  /**
   * Returns the existing (or creates a new) row delete mutation.
   * 
   * @return the existing (or creates a new) row delete mutation.
   */
  // public Delete getDelete();

  /**
   * Returns the existing (or creates a new) row increment mutation.
   * 
   * @return the existing (or creates a new) row increment mutation.
   */
  // public Increment getIncrement();

  /**
   * Creates a new row delete mutation, is not exists.
   */
  public void deleteRow();

  /**
   * Returns whether there is an existing row delete mutation.
   * 
   * @return whether there is an existing row delete mutation.
   */
  public boolean hasRowDelete();

  /**
   * Return the write operations for a row.
   * 
   * @return the write operations for a row.
   */
  public Mutations getWriteOperations();

  /**
   * Returns a single column value for this row given a context data object and
   * property. Uses a statefull column key factory to generate a column key
   * based on the given context data object and property.
   * 
   * @param dataObject
   *          the context data object
   * @param property
   *          the context property
   * @return the column value bytes
   * @throws IOException
   * 
   * @see StatefullColumnKeyFactory
   */
  public byte[] fetchColumnValue(PlasmaDataObject dataObject, PlasmaProperty property)
      throws IOException;

  /**
   * Returns the container for this writer.
   * 
   * @return the container for this writer.
   */
  public TableWriter getTableWriter();

  /**
   * Returns whether the root data object for this writer is created.
   * 
   * @return whether the root data object for this writer is created.
   */
  public boolean isRootCreated();

  /**
   * Returns whether the root data object for this writer is deleted.
   * 
   * @return whether the root data object for this writer is deleted.
   */
  public boolean isRootDeleted();

  public long newSequence(PlasmaDataObject dataObject) throws IOException;

  public void writeRowEntityMetaData(PlasmaDataObject dataObject, long sequence) throws IOException;

  public void deleteRowEntityMetaData(PlasmaDataObject dataObject, long sequence)
      throws IOException;

  public void writeRowData(PlasmaDataObject dataObject, long sequence, PlasmaProperty property,
      byte[] value) throws IOException;

  public void writeRowData(byte[] fam, byte[] qualifier, byte[] value) throws IOException;

  public void deleteRowData(PlasmaDataObject dataObject, long sequence, PlasmaProperty property)
      throws IOException;

  public void deleteRowData(byte[] fam, byte[] qualifier) throws IOException;

  public void incrementRowData(PlasmaDataObject dataObject, long sequence, PlasmaProperty property,
      long value) throws IOException;

  /**
   * Returns an existing or new edge writer for the given data object, sequence
   * and source edge property
   * 
   * @param dataObject
   *          the data object
   * @param property
   *          the source edge property
   * @param sequence
   *          the sequence for the given data object type, unique within the
   *          graph/row
   * @return an existing or new edge writer for the given data object, sequence
   *         and source edge property
   * @throws IOException
   */
  public EdgeWriter getEdgeWriter(PlasmaDataObject dataObject, PlasmaProperty property,
      long sequence) throws IOException;

  /**
   * 
   * @param dataObject
   * @param sequence
   * @param type
   */
  public void addSequence(DataObject dataObject, long sequence);

  public boolean containsSequence(DataObject dataObject);

  public long getSequence(DataObject dataObject);

  public byte[] encodeRootType();

  public byte[] encodeType(PlasmaType type);
}
