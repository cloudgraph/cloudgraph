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
package org.cloudgraph.core.graph;

import java.util.concurrent.Callable;

import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.CellValues;
import org.plasma.sdo.PlasmaDataGraph;

/**
 * A graph assembler callable task which assembles and returns a
 * {@link PlasmaDataGraph graph} on call.
 * 
 * @author Scott Cinnamond
 * @since 1.0.7
 * @see PlasmaDataGraph
 * @see CoreGraphAssembler
 */
public class GraphAssemblerCallable implements Callable<PlasmaDataGraph> {

  private Result resultRow;
  private CoreGraphAssembler graphAssembler;
  private ServiceContext serviceContext;

  public GraphAssemblerCallable(Result resultRow, CoreGraphAssembler graphAssembler,
      ServiceContext serviceContext) {
    super();
    this.resultRow = resultRow;
    this.graphAssembler = graphAssembler;
    this.serviceContext = serviceContext;
  }

  @Override
  public PlasmaDataGraph call() throws Exception {
    this.graphAssembler
        .assemble(this.serviceContext.getClientFactory().createCellValues(resultRow));
    PlasmaDataGraph graph = this.graphAssembler.getDataGraph();
    return graph;
  }

}
