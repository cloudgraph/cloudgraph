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

import java.sql.Timestamp;

import org.cloudgraph.config.CloudGraphConfigProp;
import org.cloudgraph.config.FetchType;
import org.cloudgraph.config.ThreadPoolConfigProps;
import org.cloudgraph.hbase.graph.GraphAssembler;
import org.cloudgraph.hbase.graph.GraphSliceAssembler;
import org.cloudgraph.hbase.graph.HBaseGraphAssembler;
import org.cloudgraph.hbase.graph.ParallelGraphAssembler;
import org.cloudgraph.hbase.graph.ParallelGraphSliceAssembler;
import org.cloudgraph.hbase.io.DistributedReader;
import org.plasma.query.collector.Selection;
import org.plasma.query.model.Query;
import org.plasma.sdo.PlasmaType;

/**
 * Create a specific graph assembler based on the existence of selection path
 * predicates found in the given collector. Since the reader hierarchy is
 * initialized based entirely on metadata found in the selection graph, whether
 * the persisted graph is distributed cannot be determined up front.
 * DIstribution must be discovered dynamically during assembly. Therefore graph
 * assemblers capable of handling a distributed graph are used on all cases.
 * 
 * @author Scott Cinnamond
 * @since 1.0.7
 */
public class GraphAssemblerFactory {
  private Query query;
  private PlasmaType type;
  private DistributedReader graphReader;
  private Selection selection;
  private Timestamp snapshotDate;

  @SuppressWarnings("unused")
  private GraphAssemblerFactory() {
  }

  public GraphAssemblerFactory(Query query, PlasmaType type, DistributedReader graphReader,
      Selection selection, Timestamp snapshotDate) {
    super();
    this.query = query;
    this.type = type;
    this.graphReader = graphReader;
    this.selection = selection;
    this.snapshotDate = snapshotDate;
  }

  public HBaseGraphAssembler createAssembler() {
    HBaseGraphAssembler graphAssembler = null;

    FetchType fetchType = CloudGraphConfigProp.getQueryFetchType(query);
    switch (fetchType) {
    case PARALLEL:
      ThreadPoolConfigProps config = new ThreadPoolConfigProps(query);
      switch (config.getFetchDisposition()) {
      case WIDE:
        if (selection.hasPredicates()) {
          graphAssembler = new ParallelGraphSliceAssembler(type, selection, graphReader,
              snapshotDate, config);
        } else {
          graphAssembler = new ParallelGraphAssembler(type, selection, graphReader, snapshotDate,
              config);
        }
        break;
      case TALL:
        if (selection.hasPredicates()) {
          graphAssembler = new GraphSliceAssembler(type, selection, graphReader, snapshotDate);
        } else {
          graphAssembler = new GraphAssembler(type, selection, graphReader, snapshotDate);
        }
        break;
      }
      break;
    case SERIAL:
    default:
      if (selection.hasPredicates()) {
        graphAssembler = new GraphSliceAssembler(type, selection, graphReader, snapshotDate);
      } else {
        graphAssembler = new GraphAssembler(type, selection, graphReader, snapshotDate);
      }
      break;
    }

    return graphAssembler;

  }
}
