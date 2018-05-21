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
package org.cloudgraph.hbase.service;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.plasma.sdo.PlasmaDataGraph;

/**
 * Stores intermediate results under various single and multi-scan contexts in
 * order to fulfill a query and may conditionally bypass results avoiding the
 * overhead of assembling a graph.
 * 
 * @author Scott Cinnamond
 * @since 0.5.9
 */
public interface ResultsAssembler extends ResultsSlider {
  /**
   * Returns results
   * 
   * @return
   */
  public PlasmaDataGraph[] getResults();

  public PlasmaDataGraph getCurrentResult();

  /**
   * Returns the current size of the collection
   * 
   * @return the current size of the collection
   */
  public int size();

  /**
   * Determines whether the given row is to be added to the underlying
   * collection returning true if the given row is added and false if the given
   * row is not added.
   * 
   * @param resultRow
   *          the row
   * @return true if the given row is added and false if the given row is not
   *         added.
   * @throws IOException
   */
  public boolean collect(Result resultRow) throws IOException;
}
