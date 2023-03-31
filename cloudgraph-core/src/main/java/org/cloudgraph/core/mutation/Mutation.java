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
package org.cloudgraph.core.mutation;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.io.DistributedWriter;
import org.cloudgraph.core.io.RowWriter;
import org.cloudgraph.core.io.TableWriter;
import org.plasma.sdo.PlasmaDataObject;

import commonj.sdo.DataGraph;

/**
 * Represents a mutation for a single entity within a graph.
 * 
 * @author Scott Cinnamond
 */
public interface Mutation {

  /**
   * Performs any initialization for the specific mutation to be preformed.
   * 
   * @param dataGraph
   *          the data graph
   * @param dataObject
   *          the data object
   * @param rowWriter
   *          the row writer
   * @throws IllegalAccessException
   * @throws IOException
   */
  void init(DataGraph dataGraph, PlasmaDataObject dataObject, RowWriter rowWriter)
      throws IllegalAccessException, IOException;

  /**
   * Checks for a valid mutation such as no read-only properties being modified
   * and all mandatory properties being set, etc..
   * 
   * @param dataGraph
   *          the data graph
   * @param dataObject
   *          the data object
   * @param rowWriter
   *          the row writer
   * @throws IllegalAccessException
   */
  void validate(DataGraph dataGraph, PlasmaDataObject dataObject, RowWriter rowWriter)
      throws IllegalAccessException;

  /**
   * Performs any changes on managed properties such as concurrent or locking or
   * other, etc..
   * 
   * @param dataGraph
   *          the data graph
   * @param dataObject
   *          the data object
   * @param rowWriter
   *          the row writer
   * @throws IllegalAccessException
   * @throws IOException
   */
  void setup(DataGraph dataGraph, PlasmaDataObject dataObject, RowWriter rowWriter)
      throws IllegalAccessException, IOException;

  /**
   * Interprets mutations from the data given data object (entity) as registered
   * in the associated graph change summary, and collects these into a service
   * specific mutation, an HBase row mutation in this case.
   * 
   * @param dataGraph
   *          the data graph
   * @param dataObject
   *          the data object
   * @param graphWriter
   *          the graph writer
   * @param context
   *          the table writer
   * @param rowContext
   *          the row writer
   * @throws IOException
   * @throws IllegalAccessException
   */
  void collect(DataGraph dataGraph, PlasmaDataObject dataObject, DistributedWriter graphWriter,
      TableWriter context, RowWriter rowContext) throws IOException, IllegalAccessException;

}