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
package org.cloudgraph.hbase.mutation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Row;
import org.cloudgraph.hbase.io.TableWriter;

import commonj.sdo.DataGraph;

/**
 * Traverses the change summary for one or more data graphs and collects changes
 * in the form of HBase row mutations.
 * <p>
 * For each graph: - performs any change summary ordering - collects table
 * writers based on graph metadata and configuration information - assembles
 * table writers for the graph into a single graph writer composed of table
 * writers, which are composed of row writers - passes each changed object
 * (created, modified, deleted) along with the graph writer to logic within this
 * class. - marshals out the state for each changed row after all changes
 * complete in to the state column - for each row, detects if the root object is
 * deleted, and then adds a toumbsone column
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public interface MutationCollector {

  public Map<TableWriter, List<Row>> collectChanges(DataGraph dataGraph) throws IOException,
      IllegalAccessException;

  public Map<TableWriter, List<Row>> collectChanges(DataGraph[] dataGraphs) throws IOException,
      IllegalAccessException;

  public void close();

}