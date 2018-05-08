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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.cloudgraph.hbase.connect.Connection;
import org.cloudgraph.hbase.connect.HBaseConnectionManager;
import org.cloudgraph.hbase.filter.HBaseFilterAssembler;
import org.cloudgraph.hbase.io.DistributedGraphReader;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.hbase.scan.CompleteRowKey;
import org.cloudgraph.hbase.scan.FuzzyRowKey;
import org.cloudgraph.hbase.scan.PartialRowKey;
import org.cloudgraph.hbase.scan.PartialRowKeyScanAssembler;
import org.cloudgraph.hbase.scan.ScanCollector;
import org.cloudgraph.hbase.util.FilterUtil;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprPrinter;
import org.cloudgraph.recognizer.GraphRecognizerSyntaxTreeAssembler;
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
      collectRowKeyProperties(selection, (PlasmaType) t);
    if (log.isDebugEnabled())
      log.debug(selection.dumpInheritedProperties());

    // Create and add a column filter for the initial
    // column set based on existence of path predicates
    // in the Select.
    HBaseFilterAssembler columnFilterAssembler = createRootColumnFilterAssembler(type, selection);
    Filter columnFilter = columnFilterAssembler.getFilter();

    List<PartialRowKey> partialScans = new ArrayList<PartialRowKey>();
    List<FuzzyRowKey> fuzzyScans = new ArrayList<FuzzyRowKey>();
    List<CompleteRowKey> completeKeys = new ArrayList<CompleteRowKey>();
    Expr graphRecognizerRootExpr = null;
    if (where != null) {
      GraphRecognizerSyntaxTreeAssembler recognizerAssembler = new GraphRecognizerSyntaxTreeAssembler(
          where, type);
      graphRecognizerRootExpr = recognizerAssembler.getResult();
      if (log.isDebugEnabled()) {
        ExprPrinter printer = new ExprPrinter();
        graphRecognizerRootExpr.accept(printer);
        log.debug("Graph Recognizer: " + printer.toString());
      }
      ScanCollector scanCollector = new ScanCollector(type);
      graphRecognizerRootExpr.accept(scanCollector);
      partialScans = scanCollector.getPartialRowKeyScans();
      fuzzyScans = scanCollector.getFuzzyRowKeyScans();
      completeKeys = scanCollector.getCompleteRowKeys();
      // in which case for a count this effects alot
      if (!scanCollector.isQueryRequiresGraphRecognizer())
        graphRecognizerRootExpr = null;
    }

    if (where == null
        || (partialScans.size() == 0 && fuzzyScans.size() == 0 && completeKeys.size() == 0)) {
      PartialRowKeyScanAssembler scanAssembler = new PartialRowKeyScanAssembler(type);
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

    Comparator<PlasmaDataGraph> orderingComparator = null;
    OrderBy orderBy = query.findOrderByClause();
    if (orderBy != null) {
      DataGraphComparatorAssembler orderingCompAssem = new DataGraphComparatorAssembler(
          (org.plasma.query.model.OrderBy) orderBy, type);
      orderingComparator = orderingCompAssem.getComparator();
    }

    this.executeAsStream(query, type, selection, columnFilter, graphRecognizerRootExpr,
        orderingComparator, partialScans, fuzzyScans, completeKeys, snapshotDate);
  }

  protected void executeAsStream(Query query, PlasmaType type, SelectionCollector selection,
      Filter columnFilter, Expr graphRecognizerRootExpr,
      Comparator<PlasmaDataGraph> orderingComparator, List<PartialRowKey> partialScans,
      List<FuzzyRowKey> fuzzyScans, List<CompleteRowKey> completeKeys, Timestamp snapshotDate) {
    Connection connection = HBaseConnectionManager.instance().getConnection();
    DistributedGraphReader graphReader = null;
    try {
      graphReader = new DistributedGraphReader(type, selection.getTypes(), connection);
      GraphAssemblerFactory assemblerFactory = new GraphAssemblerFactory(query, type, graphReader,
          selection, snapshotDate);
      TableReader rootTableReader = graphReader.getRootTableReader();
      ResultsAssembler resultsCollector = this.createResultsAssembler(query,
          graphRecognizerRootExpr, orderingComparator, rootTableReader, assemblerFactory);

      long before = System.currentTimeMillis();
      if (partialScans.size() > 0 || fuzzyScans.size() > 0 || completeKeys.size() > 0) {
        if (completeKeys.size() > 0) {
          throw new GraphServiceException(
              "expected scan query for streaming context - use a non-streaming API for this query");
        }
        for (PartialRowKey partialKeyScan : partialScans) {
          if (resultsCollector.isResultEndRangeReached())
            break;
          execute(partialKeyScan, rootTableReader, columnFilter, resultsCollector);
        } // scan
        for (FuzzyRowKey fuzzyScan : fuzzyScans) {
          if (resultsCollector.isResultEndRangeReached())
            break;
          execute(fuzzyScan, rootTableReader, columnFilter, resultsCollector);
        } // scan
      } else {
        FilterList rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        rootFilter.addFilter(columnFilter);
        Scan scan = new Scan();
        scan.setFilter(rootFilter);
        Float sample = query.getFromClause().getRandomSample();
        if (sample == null) {
          log.warn("query resulted in no filters or scans - using full table scan - "
              + "could result in very large results set");
        } else {
          RandomRowFilter rowFilter = new RandomRowFilter(sample);
          rootFilter.addFilter(rowFilter);
          log.warn("using random-sample scan (" + sample + ") - "
              + "could result in very large results set");
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

    if (log.isDebugEnabled())
      log.debug(FilterUtil.printFilterTree(scan.getFilter()));
    ResultScanner scanner = rootTableReader.getTable().getScanner(scan);
    try {
      for (Result resultRow : scanner) {
        if (log.isDebugEnabled()) {
          log.debug(rootTableReader.getTableConfig().getName() + ": "
              + new String(resultRow.getRow()));
          for (KeyValue keyValue : resultRow.list()) {
            log.debug("\tkey: " + new String(keyValue.getQualifier()) + "\tvalue: "
                + new String(keyValue.getValue()));
          }
        }
        if (collector.isResultEndRangeReached()) {
          break;
        }

        if (collector.collect(resultRow))
          this.emitter.onNext(collector.getCurrentResult());
      }
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  @Override
  protected StreamingResultsAssembler createResultsAssembler(Query query,
      Expr graphRecognizerRootExpr, Comparator<PlasmaDataGraph> orderingComparator,
      TableReader rootTableReader, GraphAssemblerFactory assemblerFactory) {

    StreamingResultsAssembler resultsCollector = new StreamingResultsAssembler(
        graphRecognizerRootExpr, orderingComparator, rootTableReader,
        assemblerFactory.createAssembler(), query.getStartRange(), query.getEndRange());
    return resultsCollector;
  }

}
