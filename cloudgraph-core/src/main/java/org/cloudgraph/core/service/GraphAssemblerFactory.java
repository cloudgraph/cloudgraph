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
package org.cloudgraph.core.service;

import java.sql.Timestamp;

import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.graph.GraphAssembler;
import org.cloudgraph.core.graph.GraphSliceAssembler;
import org.cloudgraph.core.graph.CoreGraphAssembler;
import org.cloudgraph.core.graph.ParallelGraphAssembler;
import org.cloudgraph.core.graph.ParallelGraphSliceAssembler;
import org.cloudgraph.core.io.DistributedReader;
import org.cloudgraph.store.mapping.FetchType;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.StoreMappingProp;
import org.cloudgraph.store.mapping.ThreadPoolMappingProps;
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
  protected ServiceContext serviceContext;

  @SuppressWarnings("unused")
  private GraphAssemblerFactory() {
  }

  public GraphAssemblerFactory(Query query, PlasmaType type, DistributedReader graphReader,
      Selection selection, Timestamp snapshotDate, ServiceContext serviceContext) {
    super();
    this.query = query;
    this.type = type;
    this.graphReader = graphReader;
    this.selection = selection;
    this.snapshotDate = snapshotDate;
    this.serviceContext = serviceContext;
  }

  public CoreGraphAssembler createAssembler() {
    CoreGraphAssembler graphAssembler = null;

    FetchType fetchType = StoreMappingProp.getQueryFetchType(query);
    switch (fetchType) {
    case PARALLEL:
      ThreadPoolMappingProps config = new ThreadPoolMappingProps(query);
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
          graphAssembler = new GraphSliceAssembler(type, this.serviceContext, selection,
              graphReader, snapshotDate);
        } else {
          graphAssembler = new GraphAssembler(type, selection, graphReader, snapshotDate);
        }
        break;
      }
      break;
    case SERIAL:
    default:
      if (selection.hasPredicates()) {
        graphAssembler = new GraphSliceAssembler(type, this.serviceContext, selection, graphReader,
            snapshotDate);
      } else {
        graphAssembler = new GraphAssembler(type, selection, graphReader, snapshotDate);
      }
      break;
    }

    return graphAssembler;

  }
}
