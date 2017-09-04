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
package org.cloudgraph.hbase.graph;

import org.apache.hadoop.hbase.client.Result;
import org.cloudgraph.hbase.io.CellValues;
import org.plasma.sdo.access.DataGraphAssembler;

/**
 * Constructs a data graph starting with a given root SDO type based on a given
 * HBase client <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Result.html"
 * >result</a> row the entire graph being found in the result row or potentially
 * requiring additional scans depending on the assembler implementation.
 * <p>
 * Since every column key in HBase must be unique, and a data graph may contain
 * any number of nodes, a column key factory is used both to persist as well as
 * re-constitute a graph. A minimal amount of "state" information is therefore
 * stored with each graph which maps user readable sequence numbers (which are
 * used in column keys) to UUID values. The nodes of the resulting data graph
 * are re-created with the original UUID values.
 * </p>
 * 
 * @see org.cloudgraph.hbase.key.StatefullColumnKeyFactory
 * @see org.plasma.sdo.PlasmaDataGraph
 * @see org.apache.hadoop.hbase.client.Result
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface HBaseGraphAssembler extends DataGraphAssembler {
  /**
   * Re-constitutes a data graph from the given HBase client result (row).
   * 
   * @param resultRow
   *          the HBase client result (row).
   */
  public void assemble(CellValues resultRow);

}