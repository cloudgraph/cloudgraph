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

import org.plasma.sdo.PlasmaType;

import commonj.sdo.DataObject;

public interface SequenceGenerator {

  // public abstract java.util.UUID getRootUUID();

  public abstract boolean isUpdated();

  public abstract void close();

  public abstract boolean hasLastSequence(PlasmaType type);

  public abstract Long lastSequence(PlasmaType type);

  /**
   * Creates and adds a sequence number mapped to the UUID within the given data
   * object.
   * 
   * @param dataObject
   *          the data object
   * @return the new sequence number
   * @throws IllegalArgumentException
   *           if the data object is already mapped
   */
  public abstract Long nextSequence(DataObject dataObject);

  public abstract Long nextSequence(PlasmaType type);

  public abstract String marshalAsString();

  public String marshalAsString(boolean formatted);

  public abstract byte[] marshal();
}