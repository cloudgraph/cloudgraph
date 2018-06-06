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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.connect.Connection;
import org.cloudgraph.hbase.connect.HBaseConnectionManager;
import org.cloudgraph.hbase.filter.GraphFetchColumnFilterAssembler;
import org.cloudgraph.hbase.filter.HBaseFilterAssembler;
import org.cloudgraph.hbase.filter.InitialFetchColumnFilterAssembler;
import org.cloudgraph.hbase.filter.PredicateRowFilterAssembler;
import org.cloudgraph.hbase.io.DistributedGraphReader;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.hbase.results.NoOpResultsComparator;
import org.cloudgraph.hbase.results.ParallelSlidingResultsAssembler;
import org.cloudgraph.hbase.results.ResultsComparatorAssembler;
import org.cloudgraph.hbase.results.ResultsAggregator;
import org.cloudgraph.hbase.results.ResultsAssembler;
import org.cloudgraph.hbase.results.ResultsComparator;
import org.cloudgraph.hbase.results.SlidingResultsAssembler;
import org.cloudgraph.hbase.scan.CompleteRowKey;
import org.cloudgraph.hbase.scan.FuzzyRowKey;
import org.cloudgraph.hbase.scan.PartialRowKey;
import org.cloudgraph.hbase.scan.PartialRowKeyScanAssembler;
import org.cloudgraph.hbase.scan.ScanCollector;
import org.cloudgraph.hbase.util.FilterUtil;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprPrinter;
import org.cloudgraph.recognizer.GraphRecognizerSyntaxTreeAssembler;
import org.cloudgraph.store.mapping.Config;
import org.cloudgraph.store.mapping.DataGraph;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.FetchType;
import org.cloudgraph.store.mapping.ParallelFetchDisposition;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingProp;
import org.cloudgraph.store.mapping.ThreadPoolMappingProps;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.From;
import org.plasma.query.model.GroupBy;
import org.plasma.query.model.Having;
import org.plasma.query.model.OrderBy;
import org.plasma.query.model.Query;
import org.plasma.query.model.Variable;
import org.plasma.query.model.Where;
import org.plasma.query.visitor.DefaultQueryVisitor;
import org.plasma.query.visitor.QueryVisitor;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.QueryDispatcher;
import org.plasma.sdo.access.provider.common.DataGraphComparatorAssembler;
import org.plasma.sdo.access.provider.common.NoOpDataGraphComparator;
import org.plasma.sdo.helper.PlasmaTypeHelper;
import org.plasma.sdo.helper.PlasmaXMLHelper;
import org.plasma.sdo.xml.DefaultOptions;
import org.xml.sax.SAXException;

import commonj.sdo.Type;
import commonj.sdo.helper.XMLDocument;

/**
 * Assembles and returns one or more {@link DataGraph data graphs} from HBase
 * given a PlasmaQuery&#8482; based XPath or DSL query. First an HBase row
 * filter is assembled using {@link PredicateRowFilterAssembler} which uses
 * query literal values and logic found in the 'where' clause or predicate(s).
 * <p>
 * Any "slice" of a graph or set of sub-graphs can be selected using the
 * PlasmaQuery&#8482; API by specifying paths through the graph. Paths may
 * include any number of predicates along the path. Based on this selection
 * criteria an {@link GraphFetchColumnFilterAssembler} is used to precisely
 * restrict the HBase columns returned for each result row.
 * </p>
 * <p>
 * Then for each resulting HBase row, a data graph
 * {@link org.cloudgraph.hbase.graph.HBaseGraphAssembler assembler} is used to
 * reconstruct and return the original graph structure from the resulting HBase
 * row.
 * </p>
 * <p>
 * The PlasmaQuery&#8482; API provides a flexible mechanism to fully describe
 * any arbitrary SDO results Data Graph, independent of any persistence
 * framework or type of data store. PlasmaQuery\u2122 supports XPath expressions
 * as a free-text "surface language", parsed by the API implementation and used
 * to construct an underlying query object model representation. As an
 * alternative to free-text, PlasmaQuery\u2122 contains a query Domain Specific
 * Language (DSL) generator and API facilitating (IDE) code-completion, 100%
 * compile-time checking and resulting in code with an almost "fluent" English
 * appearance based on your business model.
 * </p>
 * 
 * @see org.plasma.query.Query
 * @see PredicateRowFilterAssembler
 * @see SimpleGraphAssembler
 * @see GraphFetchColumnFilterAssembler
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class GraphQuery implements QueryDispatcher {
  private static Log log = LogFactory.getLog(GraphQuery.class);
  protected ServiceContext context;

  public GraphQuery(ServiceContext context) {
    this.context = context;
  }

  public void close() {
    this.context.close();
  }

  public PlasmaDataGraph[] find(Query query, Timestamp snapshotDate) {
    return find(query, -1, snapshotDate);
  }

  public PlasmaDataGraph[] find(Query query, int requestMax, Timestamp snapshotDate) {
    From from = query.getFromClause();
    if (from.getEntity() == null)
      throw new IllegalArgumentException("given query has no root type and/or URI");
    if (from.getEntity().getName() == null || from.getEntity().getNamespaceURI() == null)
      throw new IllegalArgumentException("given query has no root type and/or URI");
    PlasmaType type = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(from.getEntity()
        .getNamespaceURI(), from.getEntity().getName());
    PlasmaDataGraph[] results = new PlasmaDataGraph[0];
    try {
      results = findResults(query, type, snapshotDate);
    } finally {
    }

    return results;
  }

  /**
   * Returns a count of the given query.
   * 
   * @param query
   *          the query
   * @return the query results size
   */
  public int count(Query query) {
    From from = query.getFromClause();
    PlasmaType type = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(from.getEntity()
        .getNamespaceURI(), from.getEntity().getName());
    int size = 0;
    try {
      size = this.countResults(query, type);
    } finally {
    }

    return size;
  }

  private int countResults(Query query, PlasmaType type) {
    // FIXME: don't care about ordering, graph assembly
    // (if no graph recognizer), or potentially returning any
    // columns whatsoever.
    PlasmaDataGraph[] graphs = find(query, type, new Timestamp(System.currentTimeMillis()));
    return graphs.length;
  }

  private PlasmaDataGraph[] findResults(Query query, PlasmaType type, Timestamp snapshotDate) {
    return find(query, type, snapshotDate);
  }

  private PlasmaDataGraph[] find(Query query, PlasmaType type, Timestamp snapshotDate) {
    if (log.isDebugEnabled())
      log(query);
    Where where = query.findWhereClause();
    OrderBy orderBy = query.findOrderByClause();
    GroupBy groupBy = query.findGroupByClause();
    Having having = query.findHavingClause();

    SelectionCollector selection = new SelectionCollector(query.getSelectClause(), where, orderBy,
        groupBy, having, type);
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
    Expr whereSyntaxTree = null;
    if (where != null) {
      GraphRecognizerSyntaxTreeAssembler recognizerAssembler = new GraphRecognizerSyntaxTreeAssembler(
          where, type);
      whereSyntaxTree = recognizerAssembler.getResult();
      if (log.isDebugEnabled()) {
        ExprPrinter printer = new ExprPrinter();
        whereSyntaxTree.accept(printer);
        log.debug("Where graph Recognizer: " + printer.toString());
      }
      ScanCollector scanCollector = new ScanCollector(type);
      whereSyntaxTree.accept(scanCollector);
      partialScans = scanCollector.getPartialRowKeyScans();
      fuzzyScans = scanCollector.getFuzzyRowKeyScans();
      completeKeys = scanCollector.getCompleteRowKeys();
      // in which case for a count this effects alot
      if (!scanCollector.isQueryRequiresGraphRecognizer())
        whereSyntaxTree = null;
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

    ResultsComparator orderingComparator = null;
    if (orderBy != null) {
      ResultsComparatorAssembler orderingCompAssem = new ResultsComparatorAssembler(
          (org.plasma.query.model.OrderBy) orderBy, type);
      orderingComparator = orderingCompAssem.getComparator();
    }

    ResultsComparator groupingComparator = null;
    if (groupBy != null) {
      ResultsComparatorAssembler groupingCompAssem = new ResultsComparatorAssembler(
          (org.plasma.query.model.GroupBy) groupBy, type);
      groupingComparator = groupingCompAssem.getComparator();

    } else {
      // aggregations but no group by, give it a no-op grouping
      if (selection.hasAggregateFunctions())
        groupingComparator = new NoOpResultsComparator();
    }

    Expr havingSyntaxTree = null;
    if (having != null) {
      GraphRecognizerSyntaxTreeAssembler recognizerAssembler = new GraphRecognizerSyntaxTreeAssembler(
          having, type);
      havingSyntaxTree = recognizerAssembler.getResult();
      if (log.isDebugEnabled()) {
        ExprPrinter printer = new ExprPrinter();
        havingSyntaxTree.accept(printer);
        log.debug("Having graph Recognizer: " + printer.toString());
      }
    }

    return execute(query, type, selection, columnFilter, whereSyntaxTree, orderingComparator,
        groupingComparator, havingSyntaxTree, partialScans, fuzzyScans, completeKeys, snapshotDate);
  }

  protected PlasmaDataGraph[] execute(Query query, PlasmaType type, SelectionCollector selection,
      Filter columnFilter, Expr whereSyntaxTree, ResultsComparator orderingComparator,
      ResultsComparator groupingComparator, Expr havingSyntaxTree,
      List<PartialRowKey> partialScans, List<FuzzyRowKey> fuzzyScans,
      List<CompleteRowKey> completeKeys, Timestamp snapshotDate) {
    // execute
    Connection connection = HBaseConnectionManager.instance().getConnection();
    DistributedGraphReader graphReader = null;
    try {
      graphReader = new DistributedGraphReader(type, selection.getTypes(), connection);
      GraphAssemblerFactory assemblerFactory = new GraphAssemblerFactory(query, type, graphReader,
          selection, snapshotDate);
      TableReader rootTableReader = graphReader.getRootTableReader();
      ResultsAssembler resultsCollector = this.createResultsAssembler(query, selection,
          whereSyntaxTree, orderingComparator, groupingComparator, havingSyntaxTree,
          rootTableReader, assemblerFactory);

      long before = System.currentTimeMillis();
      if (partialScans.size() > 0 || fuzzyScans.size() > 0 || completeKeys.size() > 0) {
        if (completeKeys.size() > 0) {
          if (completeKeys.size() > 1)
            execute(completeKeys, rootTableReader, columnFilter, resultsCollector);
          else
            execute(completeKeys.get(0), rootTableReader, columnFilter, resultsCollector);
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
        log.debug("return " + String.valueOf(resultsCollector.size()) + " assembled, "
            + String.valueOf(resultsCollector.getIgnoredResults()) + " ignored, "
            + String.valueOf(resultsCollector.getUnrecognizedResults()) + " unrecognized ("
            + String.valueOf(after - before) + ")");
      }

      return resultsCollector.getResults();
    } catch (IOException e) {
      throw new GraphServiceException(e);
    } catch (Throwable t) {
      throw new GraphServiceException(t);
    } finally {
      try {
        connection.close(); // return to pool
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
      graphReader.close();
    }
  }

  protected ResultsAssembler createResultsAssembler(Query query, SelectionCollector selection,
      Expr whereSyntaxTree, ResultsComparator orderingComparator,
      ResultsComparator groupingComparator, Expr havingSyntaxTree, TableReader rootTableReader,
      GraphAssemblerFactory assemblerFactory) {

    ResultsAssembler resultsCollector = null;
    FetchType fetchType = StoreMappingProp.getQueryFetchType(query);
    switch (fetchType) {
    case PARALLEL:
      ParallelFetchDisposition fetchDisposition = StoreMappingProp
          .getQueryParallelFetchDisposition(query);
      switch (fetchDisposition) {
      case TALL: // where a thread is spawned for one or more graph
                 // roots
        // FIXME: add parallel aggregate grouping
        if (groupingComparator != null)
          log.warn("no parallel results aggregator implemented");
        resultsCollector = new ParallelSlidingResultsAssembler(whereSyntaxTree, orderingComparator,
            rootTableReader, assemblerFactory, query.getStartRange(), query.getEndRange(),
            new ThreadPoolMappingProps(query));
        break;
      default:
      }
      break;
    default:
      break;
    }

    if (resultsCollector == null) {
      if (groupingComparator == null && !selection.hasAggregateFunctions()) {
        resultsCollector = new SlidingResultsAssembler(whereSyntaxTree, orderingComparator,
            rootTableReader, assemblerFactory.createAssembler(), query.getStartRange(),
            query.getEndRange());
      } else {
        resultsCollector = new ResultsAggregator(selection, whereSyntaxTree, orderingComparator,
            groupingComparator, havingSyntaxTree, rootTableReader,
            assemblerFactory.createAssembler(), query.getStartRange(), query.getEndRange());
      }
    }
    return resultsCollector;
  }

  protected boolean canAbortScan(boolean hasOrdering, Integer startRange, Integer endRange,
      Set<PlasmaDataGraph> result) {
    if (!hasOrdering && startRange != null && endRange != null) {
      if (result.size() >= endRange.intValue())
        return true;
    }
    return false;
  }

  protected void execute(PartialRowKey partialRowKey, TableReader rootTableReader,
      Filter columnFilter, ResultsAssembler collector) throws IOException {
    Scan scan = createScan(partialRowKey, columnFilter);
    execute(scan, rootTableReader, collector);
  }

  protected Scan createScan(PartialRowKey partialRowKey, Filter columnFilter) {
    FilterList rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(columnFilter);
    Scan scan = new Scan();
    scan.setFilter(rootFilter);
    scan.setStartRow(partialRowKey.getStartKey()); // inclusive
    scan.setStopRow(partialRowKey.getStopKey()); // exclusive
    if (log.isDebugEnabled())
      log.debug("using partial row key scan: (" + "start: '" + Bytes.toString(scan.getStartRow())
          + "' stop: '" + Bytes.toString(scan.getStopRow()) + "')");
    return scan;
  }

  protected void execute(CompleteRowKey rowKey, TableReader rootTableReader, Filter columnFilter,
      ResultsAssembler collector) throws IOException {
    FilterList rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(columnFilter);
    Get get = new Get(rowKey.getKey());
    get.setFilter(columnFilter);
    if (log.isDebugEnabled())
      log.debug("using row key get: (" + "row: '" + Bytes.toString(get.getRow()) + "'");
    execute(get, rootTableReader, collector);
  }

  protected void execute(List<CompleteRowKey> rowKeys, TableReader rootTableReader,
      Filter columnFilter, ResultsAssembler collector) throws IOException {
    FilterList rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(columnFilter);
    List<Get> gets = new ArrayList<>(rowKeys.size());
    for (CompleteRowKey rowKey : rowKeys) {
      Get get = new Get(rowKey.getKey());
      get.setFilter(columnFilter);
      gets.add(get);
    }
    if (log.isDebugEnabled()) {
      StringBuilder buf = new StringBuilder();
      int i = 0;
      for (Get get : gets) {
        if (i > 0)
          buf.append(", ");
        buf.append("'");
        buf.append(Bytes.toString(get.getRow()));
        buf.append("'");
        i++;
      }
      log.debug("using row key multi-get - " + "rows: " + buf);
    }
    execute(gets, rootTableReader, collector);
  }

  protected void execute(FuzzyRowKey fuzzyScan, TableReader rootTableReader, Filter columnFilter,
      ResultsAssembler collector) throws IOException {
    Scan scan = createScan(fuzzyScan, columnFilter);
    execute(scan, rootTableReader, collector);
  }

  protected Scan createScan(FuzzyRowKey fuzzyScan, Filter columnFilter) throws IOException {
    FilterList rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    rootFilter.addFilter(columnFilter);
    Scan scan = new Scan();
    scan.setFilter(rootFilter);

    Filter fuzzyFilter = fuzzyScan.getFilter();
    rootFilter.addFilter(fuzzyFilter);
    if (log.isDebugEnabled())
      log.debug("using fuzzy scan: " + FilterUtil.printFilterTree(fuzzyFilter));

    return scan;
  }

  protected void execute(Get get, TableReader rootTableReader, ResultsAssembler collector)
      throws IOException {

    if (log.isDebugEnabled())
      log.debug("executing get...");

    if (log.isDebugEnabled())
      log.debug(FilterUtil.printFilterTree(get.getFilter()));
    Result resultRow = rootTableReader.getTable().get(get);
    if (resultRow == null || resultRow.isEmpty()) {
      log.debug("no results from table " + rootTableReader.getTableConfig().getName()
          + " for row '" + new String(get.getRow()) + "' - returning zero results graphs");
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug(rootTableReader.getTableConfig().getName() + ": " + new String(resultRow.getRow()));
      for (KeyValue keyValue : resultRow.list()) {
        log.debug("\tkey: " + new String(keyValue.getQualifier()) + "\tvalue: "
            + new String(keyValue.getValue()));
      }
    }
    collector.collect(resultRow);
  }

  protected void execute(List<Get> gets, TableReader rootTableReader, ResultsAssembler collector)
      throws IOException {

    if (log.isDebugEnabled())
      log.debug("executing multi get...");

    if (log.isDebugEnabled())
      log.debug(FilterUtil.printFilterTree(gets.get(0).getFilter()));
    Result[] resultRows = rootTableReader.getTable().get(gets);
    if (resultRows == null) {
      log.debug("no results from table " + rootTableReader.getTableConfig().getName()
          + " for mget - returning zero results graphs");
      return;
    }
    for (Result resultRow : resultRows) {
      if (!resultRow.isEmpty()) {
        if (log.isDebugEnabled()) {
          log.debug(rootTableReader.getTableConfig().getName() + ": "
              + new String(resultRow.getRow()));
          for (KeyValue keyValue : resultRow.list()) {
            log.debug("\tkey: " + new String(keyValue.getQualifier()) + "\tvalue: "
                + new String(keyValue.getValue()));
          }
        }
        if (!collector.isResultEndRangeReached())
          collector.collect(resultRow);
      } else {
        if (log.isDebugEnabled())
          log.debug("no results from table " + rootTableReader.getTableConfig().getName()
              + " for row '" + new String(resultRow.getRow()) + "' - returning no results graph");
      }
    }
  }

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

        collector.collect(resultRow);
      }
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  /**
   * Create and add a column filter for the initial column set based on
   * existence of path predicates in the Select. If no path predicates exist in
   * the selection, we know the entire selection graph can be fetched in one
   * round trip, so {@link GraphFetchColumnFilterAssembler} is create. Otherwise
   * a (@link InitialFetchColumnFilterAssembler) is used to populate the
   * root(s), and then subsequent fetches are used as directed within the graph
   * assembler facilitated by the path predicates.
   * 
   * @param type
   *          the root type
   * @param collector
   *          the selection collector
   * @return the new filter assembler
   * 
   * @see GraphFetchColumnFilterAssembler
   * @see InitialFetchColumnFilterAssembler
   */
  protected HBaseFilterAssembler createRootColumnFilterAssembler(PlasmaType type,
      SelectionCollector collector) {
    HBaseFilterAssembler columnFilterAssembler = null;
    if (collector.getPredicateMap().size() > 0) {
      columnFilterAssembler = new InitialFetchColumnFilterAssembler(collector, type);
    } else {
      columnFilterAssembler = new GraphFetchColumnFilterAssembler(collector, type);
    }
    return columnFilterAssembler;
  }

  /**
   * Recursively collects row key properties adding them to the given collector
   * for the given type. Any additional properties associated with types
   * discovered during traversal of user defined row key field paths are added
   * recursively.
   * 
   * @param collector
   *          the property collector
   * @param type
   *          the current type
   */
  protected void collectRowKeyProperties(SelectionCollector collector, PlasmaType type) {
    Config config = StoreMapping.getInstance();
    DataGraphMapping graph = config.findDataGraph(type.getQualifiedName());
    if (graph != null) {
      DataRowKeyFieldMapping[] fields = new DataRowKeyFieldMapping[graph
          .getUserDefinedRowKeyFields().size()];
      graph.getUserDefinedRowKeyFields().toArray(fields);
      for (DataRowKeyFieldMapping field : fields) {
        List<Type> types = collector.addProperty(graph.getRootType(), field.getPropertyPath());
        for (Type nextType : types)
          collectRowKeyProperties(collector, (PlasmaType) nextType);
      }
    }
  }

  protected List getVariables(Where where) {
    final List<Variable> list = new ArrayList<Variable>(1);
    QueryVisitor visitor = new DefaultQueryVisitor() {
      public void start(Variable var) {
        list.add(var);
      }
    };
    where.accept(visitor);
    return list;
  }

  protected void log(Query query) {
    String xml = "";
    PlasmaQueryDataBinding binding;
    try {
      binding = new PlasmaQueryDataBinding(new DefaultValidationEventHandler());
      xml = binding.marshal(query);
    } catch (JAXBException e) {
      log.debug(e);
    } catch (SAXException e) {
      log.debug(e);
    }
    log.debug("query: " + xml);
  }

  protected String serializeGraph(commonj.sdo.DataGraph graph) throws IOException {
    DefaultOptions options = new DefaultOptions(graph.getRootObject().getType().getURI());
    options.setRootNamespacePrefix("debug");

    XMLDocument doc = PlasmaXMLHelper.INSTANCE.createDocument(graph.getRootObject(), graph
        .getRootObject().getType().getURI(), null);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PlasmaXMLHelper.INSTANCE.save(doc, os, options);
    os.flush();
    os.close();
    String xml = new String(os.toByteArray());
    return xml;
  }
}
