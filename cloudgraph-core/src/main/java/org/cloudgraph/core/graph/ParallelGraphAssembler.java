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

import java.io.IOException;
import java.sql.Timestamp;

import org.cloudgraph.core.io.DistributedReader;
import org.cloudgraph.core.io.EdgeReader;
import org.cloudgraph.core.io.RowReader;
import org.cloudgraph.core.service.QueryThreadPool;
import org.cloudgraph.store.mapping.ThreadPoolMappingProps;
import org.plasma.query.collector.Selection;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Constructs a data graph in parallel starting with a given root using the
 * given <a target="#"
 * href="http://plasma-sdo.org/org/plasma/query/collector/Selection.html"
 * >"selection graph"</a>, where processing proceeds as a breadth-first
 * traversal and tasks are dynamically added based on thread availability within
 * a shared <a href=
 * "https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor.html"
 * >thread pool</a>.
 * <p>
 * While the result graph may be of any arbitrary size or depth, because the
 * traversal is breadth-first, many tasks are typically spawned at the "base" of
 * the graph, exhausting the available pool threads. Each subgraph task can
 * spawn further sub tasks based on thread availability, but typically this
 * means each task will traverse and process a healthy segment of the total
 * graph. Since the actual size or depth of the result graph is not known until
 * discovered on traversal, a fixed number of parallel tasks cannot be initially
 * created, but must be dynamically spawned during graph discovery.
 * <p>
 * The assembly is triggered by calling the
 * {@link ParallelGraphAssembler#assemble(Result resultRow)} method which
 * recursively reads HBase keys and values re-constituting the data graph. The
 * assembly traversal is driven by HBase column values representing the original
 * edges or containment structure of the graph.
 * </p>
 * <p>
 * Since every column key in HBase must be unique, and a data graph may contain
 * any number of nodes, a column key factory is used both to persist as well as
 * re-constitute a graph. A minimal amount of "state" information is therefore
 * stored with each graph which maps user readable sequence numbers (which are
 * used in column keys) to UUID values. The nodes of the resulting data graph
 * are re-created with the original UUID values.
 * </p>
 * 
 * @see org.cloudgraph.core.key.StatefullColumnKeyFactory
 * @see ParallelSubgraphTask
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public class ParallelGraphAssembler extends DistributedAssembler {
  /**
   * Thread pool shared by all tasks created by this assembler.
   */
  private ThreadPoolMappingProps config;

  /**
   * Constructor.
   * 
   * @param rootType
   *          the SDO root type for the result data graph
   * @param selection
   *          selected SDO properties. Properties are mapped by selected types
   *          required in the result graph.
   * @param snapshotDate
   *          the query snapshot date which is populated into every data object
   *          in the result data graph.
   * @param distributedReader
   *          the distributed reader
   * @param minPoolSize
   *          the minimum or core size of the underlying thread pool used for
   *          all tasks executed under this assembler
   * @param maxPoolSize
   *          the maximum size of the underlying thread pool used for all tasks
   *          executed under this assembler
   */
  public ParallelGraphAssembler(PlasmaType rootType, Selection selection,
      DistributedReader distributedReader, Timestamp snapshotDate, ThreadPoolMappingProps config) {
    super(rootType, selection, distributedReader, snapshotDate);

    this.config = config;
  }

  public ThreadPoolMappingProps getConfig() {
    return config;
  }

  /**
   * Creates a single task and begins the traversal from root in the current
   * thread.
   */
  @Override
  protected void assemble(PlasmaDataObject target, long targetSequence,
      EdgeReader sourceCollection, PlasmaDataObject source, PlasmaProperty sourceProperty,
      RowReader rowReader, int level) throws IOException {

    ParallelSubgraphTask task = new ParallelSubgraphTask(target, targetSequence, this.selection,
        this.snapshotDate, this.distributedReader, sourceCollection, source, sourceProperty,
        rowReader, level, 0, QueryThreadPool.instance().getExecutor(), this.config);
    task.assemble(); // in current thread.
  }
}
