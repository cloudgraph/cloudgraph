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

import static org.apache.jena.ext.com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.concurrent.SubgraphTask;
import org.cloudgraph.core.io.DistributedReader;
import org.cloudgraph.core.io.EdgeReader;
import org.cloudgraph.core.io.RowReader;
import org.cloudgraph.store.mapping.ThreadPoolMappingProps;
import org.plasma.query.collector.Selection;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Supports concurrent tasks which processes process a sub-graph starting with a
 * given "sub root" using the given <a target="#"
 * href="http://plasma-sdo.org/org/plasma/query/collector/Selection.html"
 * >"selection graph"</a>.
 * 
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
// package protection
abstract class DefaultSubgraphTask extends DistributedAssembler implements SubgraphTask {
  private static Log log = LogFactory.getLog(ParallelSubgraphTask.class);
  protected PlasmaDataObject subroot;
  protected long subrootSequence;
  protected DistributedReader distributedReader;
  protected EdgeReader edgeReader;
  protected PlasmaDataObject source;
  protected PlasmaProperty sourceProperty;
  protected RowReader rowReader;
  protected int level;
  protected int taskSequence;
  /**
   * Maps row key strings to lock objects. Where multiple threads arrive at the
   * same node and the first thread is currently processing a fetch the first
   * thread registers a lock for the row, then removes it when the fetch and any
   * subsequent shared processing is complete.
   */
  protected static Map<Integer, Object> fetchLocks = new ConcurrentHashMap<Integer, Object>();
  protected final CountDownLatch shutdownLatch = new CountDownLatch(1);
  protected ThreadPoolExecutor executorService;
  protected ThreadPoolMappingProps config;
  protected List<Traversal> traversals = new ArrayList<Traversal>();

  /**
   * Constructor.
   * 
   * @param subroot
   *          the graph sub root
   * @param selection
   *          the graph selection
   * @param snapshotDate
   *          the snapshot date
   * @param distributedReader
   *          the distributed reader
   * @param source
   *          the source data object representing the source edge
   * @param sourceProperty
   *          the source property representing the source edge
   * @param rowReader
   *          the row reader
   * @param level
   *          the traversal level
   * @param sequence
   *          the task sequence
   * @param executorService
   *          the thread pool reference
   * 
   */
  public DefaultSubgraphTask(PlasmaDataObject subroot, long subrootSequence, Selection selection,
      Timestamp snapshotDate, DistributedReader distributedReader, EdgeReader edgeReader,
      PlasmaDataObject source, PlasmaProperty sourceProperty, RowReader rowReader, int level,
      int sequence, ThreadPoolExecutor executorService, ThreadPoolMappingProps config) {
    super((PlasmaType) subroot.getType(), selection, distributedReader, snapshotDate);
    this.subroot = subroot;
    this.subrootSequence = subrootSequence;
    this.selection = selection;
    this.snapshotDate = snapshotDate;
    this.distributedReader = distributedReader;
    this.edgeReader = edgeReader;
    this.source = source;
    this.sourceProperty = sourceProperty;
    this.rowReader = rowReader;
    this.level = level;
    this.taskSequence = sequence;
    this.executorService = executorService;
    this.config = config;

    if (this.subroot == null)
      throw new IllegalArgumentException("expected arg 'subroot'");
  }

  /**
   * Starts the task.
   */
  @Override
  public void start() {
    if (log.isDebugEnabled())
      log.debug("start-" + level + "." + taskSequence);
    try {
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          // begin a breadth first traversal from the given node
          try {
            assemble(subroot, subrootSequence, edgeReader, source, sourceProperty, rowReader, level);
          } catch (IOException e) {
            log.error(e.getMessage(), e);
          }
          shutdown();
        }
      });
    } catch (RejectedExecutionException e) {
      log.error(e.getMessage(), e);
    }
  }

  /**
   * Stops the task
   */
  protected void shutdown() {
    this.shutdownLatch.countDown();
  }

  /**
   * Joins the task awaiting shutdown.
   */
  @Override
  public void join() {
    if (log.isDebugEnabled())
      log.debug("join-" + level + "." + taskSequence);
    awaitUninterruptibly(shutdownLatch);
  }

  /**
   * Launches the subroot assembly.
   * 
   * @throws IOException
   */
  public void assemble() throws IOException {
    assemble(subroot, subrootSequence, edgeReader, source, sourceProperty, rowReader, level);
  }

  /**
   * Factory method creating a new task.
   * 
   * @param subroot
   *          the graph sub root
   * @param selection
   *          the graph selection
   * @param snapshotDate
   *          the snapshot date
   * @param distributedReader
   *          the distributed reader
   * @param source
   *          the source data object representing the source edge
   * @param sourceProperty
   *          the source property representing the source edge
   * @param rowReader
   *          the row reader
   * @param level
   *          the traversal level
   * @param sequence
   *          the task sequence
   * @param executorService
   *          the thread pool reference
   * @return the task
   */
  protected abstract SubgraphTask newTask(PlasmaDataObject subroot, long subrootSequence,
      Selection selection, Timestamp snapshotDate, DistributedReader distributedReader,
      EdgeReader collection, PlasmaDataObject source, PlasmaProperty sourceProperty,
      RowReader rowReader, int level, int sequence, ThreadPoolExecutor executorService,
      ThreadPoolMappingProps config);

  @Override
  protected abstract void assemble(PlasmaDataObject target, long targetSequence,
      EdgeReader collection, PlasmaDataObject source, PlasmaProperty sourceProperty,
      RowReader rowReader, int level) throws IOException;

  /**
   * Executes the current traversals as tasks based on thread pool availability,
   * the remainder of traversals being executed within the current thread.
   * 
   * @throws IOException
   */
  protected void traverse(int level) throws IOException {
    List<Traversal> sync = new ArrayList<Traversal>();
    List<Traversal> async = new ArrayList<Traversal>();
    for (Traversal trav : this.traversals)
      if (trav.isConcurrent())
        async.add(trav);
      else
        sync.add(trav);
    this.traversals.clear();

    // create concurrent tasks based on pool availability
    int available = 0;
    if (level <= this.config.getMaxThreadDepth()) {
      available = numThreadsAvailable();
      if (available > async.size())
        available = async.size();
    }
    List<SubgraphTask> concurrentTasks = null;
    for (int i = 0; i < available; i++) {
      if (concurrentTasks == null)
        concurrentTasks = new ArrayList<SubgraphTask>();
      Traversal trav = async.get(i);
      SubgraphTask task = newTask(trav.getSubroot(), trav.getSubrootSequence(), this.selection,
          this.snapshotDate, this.distributedReader, trav.getCollection(), trav.getSource(),
          trav.getSourceProperty(), trav.getRowReader(), trav.getLevel(), concurrentTasks.size(),
          this.executorService, this.config);
      concurrentTasks.add(task);
    }
    // add remainder
    for (int i = available; i < async.size(); i++) {
      Traversal trav = async.get(i);
      sync.add(trav);
    }

    // start any asynchronous tasks
    if (concurrentTasks != null) {
      for (SubgraphTask task : concurrentTasks)
        task.start();
      for (SubgraphTask task : concurrentTasks)
        task.join();
    }

    // continue with sync tasks/traversals in this/current thread
    for (Traversal trav : sync) {
      assemble(trav.getSubroot(), trav.getSubrootSequence(), trav.getCollection(),
          trav.getSource(), trav.getSourceProperty(), trav.getRowReader(), trav.getLevel());
    }
  }

  public void logPoolStatistics() {
    if (log.isDebugEnabled())
      log.debug("active: " + this.executorService.getActiveCount() + ", size: "
          + this.executorService.getPoolSize());
  }

  /**
   * Returns true if the number of active threads have not reached the maximum
   * pool size.
   * 
   * @return true if the number of active threads have not reached the maximum
   *         pool size
   */
  public boolean threadsAvailable() {
    return this.executorService.getActiveCount() < this.executorService.getMaximumPoolSize();
  }

  /**
   * The number of available threads as the difference between the max pool and
   * the active count.
   * 
   * @return the available thread count.
   */
  public int numThreadsAvailable() {
    int result = this.executorService.getMaximumPoolSize() - this.executorService.getActiveCount();
    if (result < 0)
      result = 0;
    return result;
  }

}
