package org.cloudgraph.hbase.mapreduce;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.mapreduce.JobContext;
import org.cloudgraph.hbase.io.TableWriter;
import org.cloudgraph.hbase.mutation.GraphMutationCollector;
import org.cloudgraph.hbase.mutation.MutationCollector;
import org.cloudgraph.hbase.service.GraphQuery;
import org.cloudgraph.hbase.service.LazyServiceContext;
import org.cloudgraph.hbase.service.ServiceContext;
import org.cloudgraph.mapreduce.GraphService;
import org.cloudgraph.state.PooledStateManager;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.Query;
import org.plasma.sdo.PlasmaNode;
import org.plasma.sdo.core.SnapshotMap;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;

public class GraphServiceDelegate implements GraphService {
  private static Log log = LogFactory.getLog(GraphServiceDelegate.class);
  private ServiceContext context;

  public GraphServiceDelegate() {
    this.context = new LazyServiceContext(PooledStateManager.getInstance());
  }

  @Override
  public DataGraph[] find(Query query, JobContext jobContext) throws IOException {
    GraphQuery dispatcher = null;
    try {
      dispatcher = new GraphQuery(context);
      Timestamp timestamp = new Timestamp((new Date()).getTime());
      DataGraph[] results = dispatcher.find(query.getModel(), timestamp);
      for (DataGraph graph : results)
        graph.getChangeSummary().beginLogging();
      return results;
    } finally {
      dispatcher.close();
    }
  }

  @Override
  public void commit(DataGraph graph, JobContext jobContext) throws IOException {

    String jobName = this.getClass().getSimpleName();
    if (jobContext != null)
      jobName = jobContext.getJobName();
    SnapshotMap snapshotMap = new SnapshotMap(new Timestamp((new Date()).getTime()));
    MutationCollector collector = null;
    try {
      collector = new GraphMutationCollector(this.context, snapshotMap, jobName);

      // FIXME: if an exception happens here we don't have table writers
      // to close
      // as required by the 1.0.0 HBase client API. Will cause resource
      // bleed
      Map<TableWriter, List<Row>> mutations = new HashMap<TableWriter, List<Row>>();
      try {
        mutations = collector.collectChanges(graph);
      } catch (IllegalAccessException e) {
        throw new GraphServiceException(e);
      }

      TableWriter[] tableWriters = new TableWriter[mutations.keySet().size()];
      mutations.keySet().toArray(tableWriters);
      try {
        writeChanges(tableWriters, mutations, jobName);
      } finally {
        for (TableWriter tableWriter : tableWriters)
          tableWriter.close();
      }
      List<DataObject> changedObjects = graph.getChangeSummary().getChangedDataObjects();
      for (DataObject dataObject : changedObjects)
        if (!graph.getChangeSummary().isDeleted(dataObject))
          ((PlasmaNode) dataObject).getDataObject().reset(snapshotMap, jobName);
      graph.getChangeSummary().endLogging();
      graph.getChangeSummary().beginLogging();
    } finally {
      if (collector != null)
        collector.close();
    }
  }

  @Override
  public void commit(DataGraph[] graphs, JobContext jobContext) throws IOException {
    String jobName = this.getClass().getSimpleName();
    if (jobContext != null)
      jobName = jobContext.getJobName();
    SnapshotMap snapshotMap = new SnapshotMap(new Timestamp((new Date()).getTime()));
    Map<TableWriter, List<Row>> mutations = new HashMap<TableWriter, List<Row>>();
    MutationCollector collector = null;
    try {
      collector = new GraphMutationCollector(this.context, snapshotMap, jobName);
      try {
        mutations = collector.collectChanges(graphs);
      } catch (IllegalAccessException e) {
        throw new GraphServiceException(e);
      }
      TableWriter[] tableWriters = new TableWriter[mutations.keySet().size()];
      mutations.keySet().toArray(tableWriters);
      try {
        writeChanges(tableWriters, mutations, jobName);
      } finally {
        for (TableWriter tableWriter : tableWriters)
          tableWriter.close();
      }
      for (DataGraph graph : graphs) {
        List<DataObject> changedObjects = graph.getChangeSummary().getChangedDataObjects();
        for (DataObject dataObject : changedObjects)
          if (!graph.getChangeSummary().isDeleted(dataObject))
            ((PlasmaNode) dataObject).getDataObject().reset(snapshotMap, jobName);
        graph.getChangeSummary().endLogging();
        graph.getChangeSummary().beginLogging();
      }
    } finally {
      if (collector != null)
        collector.close();
    }
  }

  private void writeChanges(TableWriter[] tableWriters, Map<TableWriter, List<Row>> mutations,
      String jobName) throws IOException {
    for (TableWriter tableWriter : tableWriters) {
      List<Row> tableMutations = mutations.get(tableWriter);
      if (log.isDebugEnabled())
        log.debug("commiting " + tableMutations.size() + " mutations to table: "
            + tableWriter.getTableConfig().getName());
      Object[] results = new Object[tableMutations.size()];
      try {
        tableWriter.getTable().batch(tableMutations, results);
      } catch (InterruptedException e) {
        throw new GraphServiceException(e);
      }
      for (int i = 0; i < results.length; i++) {
        if (results[i] == null) {
          log.error("batch action (" + i + ") for job '" + jobName + "' failed with null result");
        } else {
          if (log.isDebugEnabled())
            log.debug("batch action (" + i + ") for job '" + jobName + "' succeeded with "
                + String.valueOf(results[i]) + " result");
        }
      }
      // tableWriter.getTable().flushCommits();
      // FIXME: find what happened to flush
    }
  }

}
