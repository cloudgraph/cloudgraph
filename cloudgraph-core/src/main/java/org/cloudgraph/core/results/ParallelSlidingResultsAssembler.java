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
package org.cloudgraph.core.results;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.graph.GraphAssemblerCallable;
import org.cloudgraph.core.graph.CoreGraphAssembler;
import org.cloudgraph.core.io.TableReader;
import org.cloudgraph.core.service.GraphAssemblerFactory;
import org.cloudgraph.core.service.QueryThreadPool;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.recognizer.GraphRecognizerContext;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.mapping.ThreadPoolMappingProps;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.sdo.PlasmaDataGraph;

/**
 * Parallel results assembler which collects callable tasks then, lazily invokes
 * the resulting {@link Future<PlasmaDataGraph> futures} before returning
 * results, the graphs being assembled by individual
 * {@link GraphAssemblerCallable} instances rather than by reusing the same
 * assembler.
 * <p>
 * </p>
 * Determines whether results can be ignored under the current context then
 * "slides" past results not within the given range, avoiding the overhead of
 * assembling a graph. When a graph recognizer is present for the current
 * context, no result can be bypassed as the recognizer requires an assembled
 * graph to evaluate and make its determination.
 * 
 * @author Scott Cinnamond
 * @since 1.0.7
 * @see GraphAssemblerCallable
 * @see CoreGraphAssembler
 * @see Expr
 * @see GraphRecognizerContext
 * @see GraphAssemblerFactory
 * @see Future<PlasmaDataGraph>
 */
public class ParallelSlidingResultsAssembler extends DefaultResultsAssembler implements
    ResultsAssembler {
  private static final Log log = LogFactory.getLog(ParallelSlidingResultsAssembler.class);
  private ThreadPoolMappingProps poolProps;
  private List<GraphAssemblerCallable> tasks;
  private GraphAssemblerFactory assemblerFactory;
  private ServiceContext serviceContext;

  public ParallelSlidingResultsAssembler(Expr graphRecognizerRootExpr,
      Comparator<PlasmaDataGraph> orderingComparator, TableReader rootTableReader,
      GraphAssemblerFactory assemblerFactory, Integer startRange, Integer endRange,
      ThreadPoolMappingProps poolProps, ServiceContext serviceContext) {
    super(graphRecognizerRootExpr, orderingComparator, rootTableReader, startRange, endRange);
    this.assemblerFactory = assemblerFactory;
    this.poolProps = poolProps;
    this.serviceContext = serviceContext;
  }

  @Override
  public boolean collect(Result resultRow) throws IOException {
    if (resultRow.containsColumn(rootTableReader.getTableConfig().getDataColumnFamilyNameBytes(),
        GraphMetaKey.TOMBSTONE.codeAsBytes())) {
      return false; // ignore toumbstone roots
    }

    if (canIgnoreResults() && currentResultIgnored()) {
      return false;
    }
    if (tasks == null)
      tasks = new ArrayList<GraphAssemblerCallable>();
    CoreGraphAssembler assembler = assemblerFactory.createAssembler();
    tasks.add(new GraphAssemblerCallable(resultRow, assembler, this.serviceContext));

    return true;
  }

  @Override
  public int size() {
    if (this.tasks != null)
      return this.tasks.size();
    else
      return 0;
  }

  @Override
  public PlasmaDataGraph[] getResults() {
    List<Future<PlasmaDataGraph>> futures = Collections.emptyList();
    try {
      futures = QueryThreadPool.instance().getExecutor().invokeAll(tasks);
    } catch (InterruptedException e) {
      throw new GraphServiceException(e);
    }

    List<PlasmaDataGraph> list = new ArrayList<>();
    for (Future<PlasmaDataGraph> future : futures) {
      PlasmaDataGraph graph = null;
      try {
        graph = future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new GraphServiceException(e);
      }

      if (this.whereSyntaxTree != null) {
        if (this.whereContext == null)
          this.whereContext = new GraphRecognizerContext();
        this.whereContext.setGraph(graph);
        if (!this.whereSyntaxTree.evaluate(this.whereContext)) {
          if (log.isDebugEnabled()) {
            log.debug("recognizer excluded: " + graph);
            log.debug(serializeGraph(graph));
          }
          this.unrecognizedResults++;
          continue; // next future
        }
      }
      list.add(graph);
    }
    PlasmaDataGraph[] array = new PlasmaDataGraph[list.size()];
    list.toArray(array);
    list.clear();

    if (this.orderingComparator != null)
      Arrays.sort(array, this.orderingComparator);
    return array;
  }

  @Override
  public PlasmaDataGraph getCurrentResult() {
    throw new IllegalStateException("not a sequential assembler");
  }

}
