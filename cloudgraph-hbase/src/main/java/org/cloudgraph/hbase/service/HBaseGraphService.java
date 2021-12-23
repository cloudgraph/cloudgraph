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

import io.reactivex.Observable;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.cloudgraph.mapreduce.GraphService;
import org.cloudgraph.store.ServiceContext;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.model.From;
import org.plasma.query.model.Query;
import org.plasma.query.model.QueryValidator;
import org.plasma.runtime.DataAccessProviderName;
import org.plasma.runtime.DataStoreType;
import org.plasma.runtime.annotation.PlasmaServiceProvider;
import org.plasma.sdo.access.AccessServiceContext;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.access.DataGraphDispatcher;
import org.plasma.sdo.access.PlasmaDataAccessService;
import org.plasma.sdo.core.SnapshotMap;
import org.plasma.sdo.helper.PlasmaTypeHelper;
import org.xml.sax.SAXException;

import commonj.sdo.DataGraph;
import commonj.sdo.Type;

/**
 * Top level provider service implementing the
 * {@link org.plasma.sdo.access.DataAccessService DataAccessService } interface
 * and delegating to {@link GraphQuery} for serving data from HBase back to the
 * client, and {@link GraphDispatcher} for propagating changes to one or more
 * data graphs back to HBase.
 * <p>
 * CloudGraph&#8482; is based on the Service Data Objects (SDO) 2.1
 * specification and is designed as a suite of SDO Data Access Services (DAS)
 * under the PlasmaSDO&#8482; Service Data Objects implementation.
 * </p>
 * <p>
 * Typical CRUD operations are provided across any full or partial data graph,
 * and any "slice" of a graph or set of sub-graph slices can be returned using
 * common SDO query mechanisms including XPath and others.
 * </p>
 * 
 * @see org.plasma.sdo.access.DataAccessService
 * @see GraphQuery
 * @see GraphDispatcher
 * @author Scott Cinnamond
 * @since 0.5
 */
@PlasmaServiceProvider(storeType = DataStoreType.NOSQL, providerName = DataAccessProviderName.HBASE)
public class HBaseGraphService implements PlasmaDataAccessService, GraphService {

  private static Log log = LogFactory.getLog(HBaseGraphService.class);
  protected ServiceContext context;

  public HBaseGraphService() {
  }

  public void initialize(AccessServiceContext context) {
    this.context = (ServiceContext) context;
  }

  public void close() {
    this.context.close();
  }

  public int count(Query query) {
    if (query == null)
      throw new IllegalArgumentException("expected non-null 'query' argument");
    validate(query);
    if (log.isDebugEnabled()) {
      log(query);
    }
    GraphQuery dispatcher = null;
    try {
      dispatcher = new GraphQuery(this.context);
      return dispatcher.count(query);
    } finally {
      if (dispatcher != null)
        dispatcher.close();
    }
  }

  public int[] count(Query[] queries) {
    if (queries == null)
      throw new IllegalArgumentException("expected non-null 'queries' argument");
    int[] counts = new int[queries.length];
    for (int i = 0; i < queries.length; i++)
      counts[i] = count(queries[i]);
    return counts;
  }

  public DataGraph[] find(Query query) {
    if (query == null)
      throw new IllegalArgumentException("expected non-null 'query' argument");
    // validate(query);
    if (log.isDebugEnabled()) {
      log(query);
    }
    GraphQuery dispatcher = null;
    try {
      dispatcher = new GraphQuery(this.context);
      Timestamp snapshotDate = new Timestamp((new Date()).getTime());
      return dispatcher.find(query, snapshotDate);
    } finally {
      if (dispatcher != null)
        dispatcher.close();
    }
  }

  public DataGraph[] find(Query query, int maxResults) {
    if (query == null)
      throw new IllegalArgumentException("expected non-null 'query' argument");
    validate(query);
    if (log.isDebugEnabled()) {
      log(query);
    }
    GraphQuery graphQuery = null;
    try {
      graphQuery = new GraphQuery(this.context);
      DataGraph[] results = graphQuery.find(query, -1, new Timestamp((new Date()).getTime()));
      return results;
    } finally {
      if (graphQuery != null)
        graphQuery.close();
    }
  }

  public List<DataGraph[]> find(Query[] queries) {
    if (queries == null)
      throw new IllegalArgumentException("expected non-null 'queries' argument");
    GraphQuery graphQuery = null;
    try {
      graphQuery = new GraphQuery(this.context);
      List<DataGraph[]> list = new ArrayList<DataGraph[]>();
      Timestamp snapshotDate = new Timestamp((new Date()).getTime());
      for (int i = 0; i < queries.length; i++) {
        validate(queries[i]);
        if (log.isDebugEnabled()) {
          log(queries[i]);
        }
        DataGraph[] results = graphQuery.find(queries[i], snapshotDate);
        list.add(results);
      }
      return list;
    } finally {
      if (graphQuery != null)
        graphQuery.close();
    }
  }

  public SnapshotMap commit(DataGraph dataGraph, String username) {
    if (dataGraph == null)
      throw new IllegalArgumentException("expected non-null 'dataGraph' argument");
    if (username == null)
      throw new IllegalArgumentException("expected non-null 'username' argument");
    if (username.trim().length() == 0)
      throw new IllegalArgumentException("unexpected zero length 'username' argument");
    SnapshotMap snapshotMap = new SnapshotMap(new Timestamp((new Date()).getTime()));
    DataGraphDispatcher dispatcher = null;
    try {
      dispatcher = new GraphDispatcher(this.context, snapshotMap, username);
      dispatcher.commit(dataGraph);
      return snapshotMap;
    } catch (DataAccessException e) {
      log.error(e.getMessage(), e);
      throw e;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new DataAccessException(e);
    } finally {
      if (dispatcher != null)
        dispatcher.close();
    }
  }

  public SnapshotMap commit(DataGraph[] dataGraphs, String username) {
    if (dataGraphs == null)
      throw new IllegalArgumentException("expected non-null 'dataGraphs' argument");
    if (username == null)
      throw new IllegalArgumentException("expected non-null 'username' argument");
    if (username.trim().length() == 0)
      throw new IllegalArgumentException("unexpected zero length 'username' argument");

    SnapshotMap snapshotMap = new SnapshotMap(new Timestamp((new Date()).getTime()));
    DataGraphDispatcher dispatcher = null;

    try {
      dispatcher = new GraphDispatcher(this.context, snapshotMap, username);
      dispatcher.commit(dataGraphs);
      return snapshotMap;
    } catch (DataAccessException e) {
      throw e;
    } catch (Exception t) {
      throw new DataAccessException(t);
    } finally {
      if (dispatcher != null)
        dispatcher.close();
    }
  }

  protected void validate(Query query) {
    From from = (From) query.getFromClause();
    Type type = PlasmaTypeHelper.INSTANCE.getType(from.getEntity().getNamespaceURI(), from
        .getEntity().getName());
    if (log.isDebugEnabled())
      log.debug("validating query");
    new QueryValidator((Query) query, type);
  }

  protected void log(Query query) {
    if (log.isDebugEnabled()) {
      String xml = "";
      PlasmaQueryDataBinding binding;
      try {
        binding = new PlasmaQueryDataBinding(new DefaultValidationEventHandler());
        xml = binding.marshal(query);
      } catch (JAXBException e) {
      } catch (SAXException e) {
      }
      log.debug("query: " + xml);
    }
  }

  @Override
  public DataGraph[] find(org.plasma.query.Query query, JobContext context) throws IOException {

    return this.find(query.getModel());
  }

  @Override
  public void commit(DataGraph graph, JobContext context) throws IOException {
    String username = "graph-service";
    if (context != null)
      username = context.getJobName();
    this.commit(graph, username);
  }

  @Override
  public void commit(DataGraph[] graphs, JobContext context) throws IOException {
    String username = "graph-service";
    if (context != null)
      username = context.getJobName();
    this.commit(graphs, username);
  }

  @Override
  public Observable<DataGraph> findAsStream(Query query) {
    if (query == null)
      throw new IllegalArgumentException("expected non-null 'query' argument");
    // validate(query);
    if (log.isDebugEnabled()) {
      log(query);
    }
    GraphStreamQuery dispatcher = null;
    try {
      dispatcher = new GraphStreamQuery(this.context);
      Timestamp snapshotDate = new Timestamp((new Date()).getTime());
      return dispatcher.findAsStream(query, snapshotDate);
    } finally {
      if (dispatcher != null)
        dispatcher.close();
    }
  }

  @Override
  public Observable<DataGraph> findAsStream(Query query, int maxResults) {
    if (query == null)
      throw new IllegalArgumentException("expected non-null 'query' argument");
    validate(query);
    if (log.isDebugEnabled()) {
      log(query);
    }
    GraphStreamQuery graphQuery = null;
    try {
      graphQuery = new GraphStreamQuery(this.context);
      return graphQuery.findAsStream(query, -1, new Timestamp((new Date()).getTime()));
    } finally {
      if (graphQuery != null)
        graphQuery.close();
    }
  }

  @Override
  public List<Observable<DataGraph>> findAsStream(Query[] queries) {
    // TODO Auto-generated method stub
    return null;
  }
}
