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
package org.cloudgraph.hbase.mapreduce;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.mapreduce.JobContext;
import org.cloudgraph.core.Connection;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.Row;
import org.cloudgraph.core.io.TableWriter;
import org.cloudgraph.core.mutation.GraphMutationCollector;
import org.cloudgraph.core.mutation.GraphMutationWriter;
import org.cloudgraph.core.mutation.MutationCollector;
import org.cloudgraph.core.mutation.Mutations;
import org.cloudgraph.core.service.GraphQuery;
import org.cloudgraph.hbase.service.HBaseServiceContext;
import org.cloudgraph.mapreduce.GraphService;
import org.cloudgraph.store.mapping.ConfigurationProperty;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.Query;
import org.plasma.sdo.PlasmaNode;
import org.plasma.sdo.core.SnapshotMap;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;

public class GraphServiceDelegate implements GraphService {
  private static Log log = LogFactory.getLog(GraphServiceDelegate.class);
  private ServiceContext context;

  @SuppressWarnings("unused")
  private GraphServiceDelegate() {
  }

  public GraphServiceDelegate(ServiceContext context) {
    this.context = context;
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
    ServiceContext serviceContext = createServiceContext(jobContext);

    String jobName = this.getClass().getSimpleName();
    if (jobContext != null)
      jobName = jobContext.getJobName();
    SnapshotMap snapshotMap = new SnapshotMap(new Timestamp((new Date()).getTime()));
    MutationCollector collector = null;
    Connection connection = serviceContext.getConnectionManager().getConnection();
    try {
      collector = new GraphMutationCollector(this.context, snapshotMap, jobName);

      // FIXME: if an exception happens here we don't have table writers
      // to close
      // as required by the 1.0.0 HBase client API. Will cause resource
      // bleed
      // Map<TableWriter, List<Row>> mutations = new HashMap<TableWriter,
      // List<Row>>();
      Map<TableWriter, Map<String, Mutations>> mutations = new HashMap<>();
      try {
        mutations = collector.collectChanges(graph, connection);
      } catch (IllegalAccessException e) {
        throw new GraphServiceException(e);
      }

      TableWriter[] tableWriters = new TableWriter[mutations.keySet().size()];
      mutations.keySet().toArray(tableWriters);
      GraphMutationWriter writer = new GraphMutationWriter(serviceContext);
      try {
        writer.writeChanges(tableWriters, mutations, snapshotMap, jobName);
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
      try {
        connection.close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
      if (collector != null)
        collector.close();
    }
  }

  @Override
  public void commit(DataGraph[] graphs, JobContext jobContext) throws IOException {
    String jobName = jobContext.getJobName();
    ServiceContext serviceContext = createServiceContext(jobContext);
    SnapshotMap snapshotMap = new SnapshotMap(new Timestamp((new Date()).getTime()));
    // Map<TableWriter, List<Row>> mutations = new HashMap<TableWriter,
    // List<Row>>();
    Map<TableWriter, Map<String, Mutations>> mutations = new HashMap<>();
    MutationCollector collector = null;
    Connection connection = serviceContext.getConnectionManager().getConnection();
    try {
      collector = new GraphMutationCollector(this.context, snapshotMap, jobName);
      try {
        mutations = collector.collectChanges(graphs, connection);
      } catch (IllegalAccessException e) {
        throw new GraphServiceException(e);
      }
      TableWriter[] tableWriters = new TableWriter[mutations.keySet().size()];
      mutations.keySet().toArray(tableWriters);

      GraphMutationWriter writer = new GraphMutationWriter(serviceContext);
      try {
        writer.writeChanges(tableWriters, mutations, snapshotMap, jobName);
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
      try {
        connection.close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
      if (collector != null)
        collector.close();
    }
  }

  private StoreMappingContext createMappingContext(JobContext jobContext) {
    Properties mappingProps = new Properties();
    String rootPath = jobContext.getConfiguration().get(
        ConfigurationProperty.CLOUDGRAPH___ROOT___TABLE___PATH___PREFIX.value());
    if (rootPath != null)
      mappingProps.setProperty(
          ConfigurationProperty.CLOUDGRAPH___ROOT___TABLE___PATH___PREFIX.value(), rootPath);
    String volume = jobContext.getConfiguration().get(
        ConfigurationProperty.CLOUDGRAPH___VOLUME___PATH___PREFIX.value());
    if (volume != null)
      mappingProps.setProperty(ConfigurationProperty.CLOUDGRAPH___VOLUME___PATH___PREFIX.value(),
          volume);
    return new StoreMappingContext(mappingProps);
  }

  private ServiceContext createServiceContext(JobContext jobContext) {
    Properties mappingProps = new Properties();
    String rootPath = jobContext.getConfiguration().get(
        ConfigurationProperty.CLOUDGRAPH___ROOT___TABLE___PATH___PREFIX.value());
    if (rootPath != null)
      mappingProps.setProperty(
          ConfigurationProperty.CLOUDGRAPH___ROOT___TABLE___PATH___PREFIX.value(), rootPath);
    String volume = jobContext.getConfiguration().get(
        ConfigurationProperty.CLOUDGRAPH___VOLUME___PATH___PREFIX.value());
    if (volume != null)
      mappingProps.setProperty(ConfigurationProperty.CLOUDGRAPH___VOLUME___PATH___PREFIX.value(),
          volume);

    return new HBaseServiceContext(mappingProps);
  }

  private void writeChanges(TableWriter[] tableWriters, Map<TableWriter, List<Row>> mutations,
      String jobName) throws IOException {
    for (TableWriter tableWriter : tableWriters) {
      List<Row> tableMutations = mutations.get(tableWriter);
      if (log.isDebugEnabled())
        log.debug("commiting " + tableMutations.size() + " mutations to table: "
            + tableWriter.getTableConfig().getNamespaceQualifiedPhysicalName());
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
