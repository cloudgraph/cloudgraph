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
package org.cloudgraph.rocksdb.service;

// java imports
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprPrinter;
import org.cloudgraph.recognizer.GraphRecognizerSyntaxTreeAssembler;
import org.cloudgraph.rocksdb.connect.RocksDBConnectionManager;
import org.cloudgraph.rocksdb.connect.Connection;
import org.cloudgraph.rocksdb.ext.Result;
import org.cloudgraph.rocksdb.ext.Scan;
import org.cloudgraph.rocksdb.filter.Filter;
import org.cloudgraph.rocksdb.filter.FilterList;
import org.cloudgraph.rocksdb.filter.RocksDBFilterAssembler;
import org.cloudgraph.rocksdb.io.DistributedGraphReader;
import org.cloudgraph.rocksdb.io.TableReader;
import org.cloudgraph.rocksdb.results.ResultsAssembler;
import org.cloudgraph.rocksdb.results.ResultsComparator;
import org.cloudgraph.rocksdb.results.ResultsComparatorAssembler;
import org.cloudgraph.rocksdb.results.StreamingResultsAssembler;
import org.cloudgraph.rocksdb.scan.CompleteRowKey;
import org.cloudgraph.rocksdb.scan.PartialRowKey;
import org.cloudgraph.rocksdb.scan.PartialRowKeyScanAssembler;
import org.cloudgraph.rocksdb.scan.ScanCollector;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.OrderBy;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.From;
import org.plasma.query.model.Query;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.StreamQueryDispatcher;
import org.plasma.sdo.access.provider.common.DataGraphComparatorAssembler;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.Type;

public class GraphStreamQuery extends GraphQuery implements
    ObservableOnSubscribe<commonj.sdo.DataGraph>, StreamQueryDispatcher {
  private static Log log = LogFactory.getLog(GraphStreamQuery.class);
  private Query query;
  private Timestamp snapshotDate;
  private int requestMax;
  private ObservableEmitter<commonj.sdo.DataGraph> emitter;

  public GraphStreamQuery(ServiceContext context) {
    super(context);
  }

  @Override
  public void close() {
    this.context.close();
  }

  @Override
  public Observable<commonj.sdo.DataGraph> findAsStream(Query query, Timestamp snapshotDate) {
    this.query = query;
    this.snapshotDate = snapshotDate;
    this.requestMax = -1;
    return Observable.create(this);
  }

  public Observable<commonj.sdo.DataGraph> findAsStream(Query query, int requestMax,
      Timestamp snapshotDate) {
    this.query = query;
    this.snapshotDate = snapshotDate;
    this.requestMax = requestMax;
    return Observable.create(this);
  }

  @Override
  public void subscribe(ObservableEmitter<commonj.sdo.DataGraph> emitter) throws Exception {
    this.emitter = emitter;
    From from = this.query.getFromClause();
    PlasmaType type = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(from.getEntity()
        .getNamespaceURI(), from.getEntity().getName());

    Where where = query.findWhereClause();

    SelectionCollector selection = null;
    if (where != null)
      selection = new SelectionCollector(query.getSelectClause(), where, type);
    else
      selection = new SelectionCollector(query.getSelectClause(), type);
    selection.setOnlyDeclaredProperties(false);
    for (Type t : selection.getTypes())
      collectRowKeyProperties(selection, (PlasmaType) t, this.context);
    if (log.isDebugEnabled())
      log.debug(selection.dumpInheritedProperties());

    // Create and add a column filter for the initial
    // column set based on existence of path predicates
    // in the Select.
    RocksDBFilterAssembler columnFilterAssembler = createRootColumnFilterAssembler(type, selection,
        this.context);
    Filter columnFilter = columnFilterAssembler.getFilter();

    List<PartialRowKey> partialScans = new ArrayList<PartialRowKey>();
    List<CompleteRowKey> completeKeys = new ArrayList<CompleteRowKey>();
    Expr whereSyntaxTree = null;
    if (where != null) {
      GraphRecognizerSyntaxTreeAssembler recognizerAssembler = new GraphRecognizerSyntaxTreeAssembler(
          where, type);
      whereSyntaxTree = recognizerAssembler.getResult();
      if (log.isDebugEnabled()) {
        ExprPrinter printer = new ExprPrinter();
        whereSyntaxTree.accept(printer);
        log.debug("Graph Recognizer: " + printer.toString());
      }
      ScanCollector scanCollector = new ScanCollector(type, this.context.getStoreMapping());
      whereSyntaxTree.accept(scanCollector);
      partialScans = scanCollector.getPartialRowKeyScans();
      completeKeys = scanCollector.getCompleteRowKeys();
      // in which case for a count this effects alot
      if (!scanCollector.isQueryRequiresGraphRecognizer())
        whereSyntaxTree = null;
    }

    if (where == null || (partialScans.size() == 0 && completeKeys.size() == 0)) {
      PartialRowKeyScanAssembler scanAssembler = new PartialRowKeyScanAssembler(type,
          this.context.getStoreMapping());
      scanAssembler.assemble();
      byte[] startKey = scanAssembler.getStartKey();
      if (startKey != null && startKey.length > 0) {
        if ((query.getStartRange() == null || query.getStartRange() <= 0)
            && (query.getEndRange() == null || query.getEndRange() <= 0)) {
          log.warn("no root predicate or range limit present - using default graph partial "
              + "key scan - could result in very large results set");
        }
        partialScans.add(scanAssembler);
      }
    }

    ResultsComparator orderingComparator = null;
    OrderBy orderBy = query.findOrderByClause();
    if (orderBy != null) {
      ResultsComparatorAssembler orderingCompAssem = new ResultsComparatorAssembler(
          (org.plasma.query.model.OrderBy) orderBy, type);
      orderingComparator = orderingCompAssem.getComparator();
    }

    this.executeAsStream(query, selection, type, columnFilter, whereSyntaxTree, orderingComparator,
        partialScans, completeKeys, snapshotDate);
  }

  protected void executeAsStream(Query query, SelectionCollector selection, PlasmaType type,
      Filter columnFilter, Expr whereSyntaxTree, ResultsComparator orderingComparator,
      List<PartialRowKey> partialScans, List<CompleteRowKey> completeKeys, Timestamp snapshotDate) {
    Connection connection = RocksDBConnectionManager.instance().getConnection();
    DistributedGraphReader graphReader = null;
    try {
      graphReader = new DistributedGraphReader(type, selection.getTypes(), connection,
          this.context.getStoreMapping());
      GraphAssemblerFactory assemblerFactory = new GraphAssemblerFactory(query, type, graphReader,
          selection, snapshotDate, this.context.getStoreMapping());
      TableReader rootTableReader = graphReader.getRootTableReader();
      ResultsAssembler resultsCollector = this.createResultsAssembler(query, selection,
          whereSyntaxTree, orderingComparator, null, null, rootTableReader, assemblerFactory);

      long before = System.currentTimeMillis();
      if (partialScans.size() > 0 || completeKeys.size() > 0) {
        if (completeKeys.size() > 0) {
          throw new GraphServiceException(
              "expected scan query for streaming context - use a non-streaming API for this query");
        }
        for (PartialRowKey partialKeyScan : partialScans) {
          if (resultsCollector.isResultEndRangeReached())
            break;
          execute(partialKeyScan, rootTableReader, columnFilter, resultsCollector);
        } // scan
      } else {
        Scan scan = new Scan(columnFilter);
        Float sample = query.getFromClause().getRandomSample();
        if (sample == null) {
          log.warn("query resulted in no filters or scans - using full table scan - "
              + "could result in very large results set");
        } else {
          throw new GraphServiceException("scans with sampling not supported");
        }
        execute(scan, rootTableReader, resultsCollector);
      }

      if (log.isDebugEnabled()) {
        long after = System.currentTimeMillis();
        log.debug("stream complete " + String.valueOf(resultsCollector.size()) + " assembled, "
            + String.valueOf(resultsCollector.getIgnoredResults()) + " ignored, "
            + String.valueOf(resultsCollector.getUnrecognizedResults()) + " unrecognized ("
            + String.valueOf(after - before) + ")");
      }

    } catch (IOException e) {
      this.emitter.onError(new GraphServiceException(e));
    } catch (Throwable t) {
      this.emitter.onError(new GraphServiceException(t));
    } finally {
      try {
        connection.close(); // return to pool
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
      graphReader.close();
      this.emitter.onComplete();
    }
  }

  @Override
  protected void execute(Scan scan, TableReader rootTableReader, ResultsAssembler collector)
      throws IOException {

    if (log.isDebugEnabled())
      log.debug("executing scan...");
    Result[] recordSet = rootTableReader.getTable().scan(scan);
    for (Result resultRow : recordSet) {
      if (log.isDebugEnabled()) {
        log.debug(rootTableReader.getTableConfig().getQualifiedPhysicalName() + ": "
            + new String(resultRow.getRow()));
      }
      if (collector.isResultEndRangeReached()) {
        break;
      }

      if (collector.collect(resultRow))
        this.emitter.onNext(collector.getCurrentResult());
    }
  }

  @Override
  protected ResultsAssembler createResultsAssembler(Query query, SelectionCollector selection,
      Expr whereSyntaxTree, ResultsComparator orderingComparator,
      ResultsComparator groupingComparator, Expr havingSyntaxTree, TableReader rootTableReader,
      GraphAssemblerFactory assemblerFactory) {

    StreamingResultsAssembler resultsCollector = new StreamingResultsAssembler(whereSyntaxTree,
        orderingComparator, rootTableReader, assemblerFactory.createAssembler(),
        query.getStartRange(), query.getEndRange());
    return resultsCollector;
  }

}
