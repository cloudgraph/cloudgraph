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
package org.cloudgraph.hbase.graph;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
//import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.hbase.filter.BinaryPrefixColumnFilterAssembler;
import org.cloudgraph.hbase.filter.ColumnPredicateFilterAssembler;
import org.cloudgraph.hbase.filter.GraphFetchColumnFilterAssembler;
import org.cloudgraph.hbase.filter.HBaseFilterAssembler;
import org.cloudgraph.hbase.filter.PredicateFilterAssembler;
import org.cloudgraph.hbase.filter.PredicateUtil;
import org.cloudgraph.hbase.filter.StatefullBinaryPrefixColumnFilterAssembler;
import org.cloudgraph.hbase.io.CellValues;
import org.cloudgraph.hbase.io.DistributedGraphReader;
import org.cloudgraph.hbase.io.DistributedReader;
import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.io.RowReader;
import org.cloudgraph.hbase.io.TableOperation;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.hbase.util.FilterUtil;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprPrinter;
import org.cloudgraph.recognizer.Endpoint;
import org.cloudgraph.recognizer.GraphRecognizerContext;
import org.cloudgraph.recognizer.GraphRecognizerSyntaxTreeAssembler;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.collector.Selection;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.Query;
import org.plasma.query.model.Where;
import org.plasma.sdo.Key;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.helper.PlasmaXMLHelper;
import org.plasma.sdo.profile.KeyStructure;
import org.plasma.sdo.xml.DefaultOptions;
import org.xml.sax.SAXException;

import commonj.sdo.Property;
import commonj.sdo.helper.XMLDocument;

/**
 * Delegate class for various graph slice fetch and edge post processing
 * operations. Supports graph assemblers and other clients.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
class GraphSliceSupport {
  private static Log log = LogFactory.getLog(GraphSliceSupport.class);
  private Selection selection;
  private Timestamp snapshotDate;
  private Charset charset;

  @SuppressWarnings("unused")
  private GraphSliceSupport() {
  }

  public GraphSliceSupport(Selection selection, Timestamp snapshotDate) {
    this.selection = selection;
    this.snapshotDate = snapshotDate;
    this.charset = Charset.forName(CoreConstants.UTF8_ENCODING);
  }

  /**
   * Filters edges in two stages, first constructs an edge recognizer which
   * interrogates each row key in the given edge collection to determine if it
   * satisfies the given predicate tree. This step is performed in memory only,
   * no RPC calls are needed. Then for the filtered results performs a graph
   * assembly for each external edge, then uses a graph recognizer to determine
   * if the assembled graph satisfies the given predicate tree
   * 
   * @param contextType
   *          the current type
   * @param edgeReader
   *          the edge collection
   * @param where
   *          the predicate structure
   * @param rowReader
   *          the row reader
   * @param tableReader
   *          the table reader
   * @return the filtered results
   * @throws IOException
   * @throws IllegalStateException
   *           if the edge collection is not external
   */
  public List<CellValues> filter(PlasmaType contextType, EdgeReader edgeReader, Where where,
      RowReader rowReader, TableReader tableReader) throws IOException {
    List<CellValues> results = new ArrayList<>((int) edgeReader.getCount());

    if (!edgeReader.isExternal())
      throw new IllegalStateException("expected external edge collection not, " + edgeReader);

    if (log.isDebugEnabled())
      log(where);
    List<String> graphEvalRowKeys = new ArrayList<>();
    ExternalEdgeRecognizerSyntaxTreeAssembler edgeSyntaxAssembler = new ExternalEdgeRecognizerSyntaxTreeAssembler(
        where, rowReader.getDataGraph(), contextType, rowReader.getRootType());
    Expr edgeRecognizerRootExpr = edgeSyntaxAssembler.getResult();
    if (log.isDebugEnabled()) {
      ExprPrinter printer = new ExprPrinter();
      edgeRecognizerRootExpr.accept(printer);
      log.debug("Edge Recognizer: " + printer.toString());
    }
    CellConverter cellConverter = new CellConverter(contextType, tableReader.getTableConfig());
    ExternalEdgeRecognizerContext edgeRecogniserContext = new ExternalEdgeRecognizerContext(
        contextType);
    boolean complete = rowKeysHaveSelection(contextType, edgeRecogniserContext.getEndpoints());
    for (String rowKey : edgeReader.getRowKeys()) {
      edgeRecogniserContext.read(rowKey);
      if (edgeRecognizerRootExpr.evaluate(edgeRecogniserContext)) {
        if (!edgeRecogniserContext.isRowEvaluatedCompletely()) {
          graphEvalRowKeys.add(rowKey);
        } else {
          CellValues cellValues = cellConverter.convert(rowKey, edgeRecogniserContext.getValues());
          cellValues.setCompleteSelection(complete);
          results.add(cellValues);
        }
      }
    }
    if (log.isDebugEnabled())
      log.debug("recognized " + graphEvalRowKeys.size() + results.size() + " out of "
          + edgeReader.getRowKeys().size() + " external edges");
    if (graphEvalRowKeys.size() == 0)
      return results; // no edges recognized or all recognized by row key alone

    SelectionCollector predicateSelection = new SelectionCollector(where, contextType);

    // create a new reader as the existing one may have cached data objects
    // already
    // linked to the parent graph. Cannot link to sub-graph as well.
    DistributedReader existingReader = (DistributedReader) tableReader.getDistributedOperation();
    DistributedGraphReader sliceGraphReader = new DistributedGraphReader(contextType,
        predicateSelection.getTypes(), existingReader.getMarshallingContext());

    HBaseGraphAssembler graphAssembler = new GraphAssembler(contextType, predicateSelection,
        sliceGraphReader, snapshotDate);

    GraphRecognizerSyntaxTreeAssembler recognizerAssembler = new GraphRecognizerSyntaxTreeAssembler(
        where, contextType);
    Expr graphRecognizerRootExpr = recognizerAssembler.getResult();
    if (log.isDebugEnabled()) {
      ExprPrinter printer = new ExprPrinter();
      graphRecognizerRootExpr.accept(printer);
      log.debug("Graph Recognizer: " + printer.toString());
    }

    // column filter
    HBaseFilterAssembler columnFilterAssembler = new GraphFetchColumnFilterAssembler(
        predicateSelection, contextType); // use predicate selection not entire
                                          // graph selection
    Filter columnFilter = columnFilterAssembler.getFilter();

    List<Get> gets = new ArrayList<Get>();
    for (String rowKey : graphEvalRowKeys) {
      // byte[] childRowKey =
      // rowReader.getGraphState().getRowKey(edge.getUuid()); // use local
      // edge UUID
      Get get = new Get(Bytes.toBytes(rowKey));
      get.setFilter(columnFilter);
      gets.add(get);
    }
    DataGraphConfig graphConfig = CloudGraphConfig.getInstance().getDataGraph(
        contextType.getQualifiedName());
    Result[] rows = this.fetchResult(gets, tableReader, graphConfig);

    GraphRecognizerContext recognizerContext = new GraphRecognizerContext();
    int rowIndex = 0;
    for (Result resultRow : rows) {
      if (resultRow == null || resultRow.isEmpty()) {
        Get get = gets.get(rowIndex);
        String rowStr = new String(get.getRow(), charset);
        if (resultRow == null)
          throw new IllegalStateException("got null result row for '" + rowStr
              + "' for mulit-get operation - indicates failure with retries");
        else
          throw new IllegalStateException("got no result for row for '" + rowStr
              + "' for mulit-get operation - indicates row noes not exist");
      }
      // Assemble a predicate slice graph where the edge is root
      // Can be any size and have both local and external edges
      graphAssembler.assemble(new CellValues(resultRow));
      PlasmaDataGraph assembledGraph = graphAssembler.getDataGraph();
      graphAssembler.clear();

      recognizerContext.setGraph(assembledGraph);
      if (!graphRecognizerRootExpr.evaluate(recognizerContext)) {
        if (log.isDebugEnabled())
          log.debug("recognizer excluded: " + Bytes.toString(resultRow.getRow()));
        if (log.isDebugEnabled())
          log.debug(serializeGraph(assembledGraph));

        continue;
      }

      String rowKey = new String(resultRow.getRow(), charset);
      CellValues values = new CellValues(rowKey);
      values.setCompleteSelection(false);
      results.add(values);
      rowIndex++;
    }

    return results;
  }

  /**
   * 
   * @param contextType
   * @param endpoints
   * @return
   */
  private boolean rowKeysHaveSelection(PlasmaType contextType, Collection<Endpoint> endpoints) {

    boolean hasRootContextUuid = false;
    for (Endpoint endpoint : endpoints) {
      Key key = endpoint.getProperty().getKey();
      if (key != null && key.getStructure() != null && 
          KeyStructure.valueOf(key.getStructure().name()).ordinal() == KeyStructure.uuid.ordinal()) {
        if (endpoint.getProperty().getContainingType().equals(contextType)) {
          hasRootContextUuid = true;
          break;
        }
      }
    }
    if (!hasRootContextUuid)
      return false;

    for (Property prop : this.selection.getProperties(contextType)) {
      PlasmaProperty plasmaProp = (PlasmaProperty) prop;
      boolean found = false;
      for (Endpoint endpoint : endpoints) {
        if (endpoint.getProperty().equals(plasmaProp)) {
          found = true;
          break;
        }
      }
      if (!found)
        return false;
    }
    return true;
  }

  /**
   * Creates a column qualifier/value filter hierarchy based on the given path
   * predicate for a single row specified by the given row key, then returns the
   * column qualifier sequence numbers which represent the subset of total graph
   * edges as restricted by the predicate.
   * 
   * @param contextType
   *          the type of the edge property
   * @param where
   *          the predicate
   * @param rowKey
   *          the row key
   * @return a collection of sequence ids
   * @throws IOException
   * @see ColumnPredicateFilterAssembler
   */
  public Set<Long> fetchSequences(PlasmaType contextType, Where where, RowReader rowReader)
      throws IOException {

    PlasmaType rootType = (PlasmaType) rowReader.getRootType();
    DataGraphConfig graphConfig = CloudGraphConfig.getInstance().getDataGraph(
        rootType.getQualifiedName());
    Get get = new Get(rowReader.getRowKey());

    PredicateUtil predicateUtil = new PredicateUtil();
    PredicateFilterAssembler filterAssembler = null;
    boolean multiDescendantProperties = predicateUtil.hasHeterogeneousDescendantProperties(where);
    // if (!multiDescendantProperties) {
    filterAssembler = new ColumnPredicateFilterAssembler(rootType);
    // }
    // else {
    // filterAssembler = new
    // MultiColumnPredicateFilterAssembler(rowReader.getGraphState(),
    // rootType);
    // }
    filterAssembler.assemble(where, contextType);
    Filter filter = filterAssembler.getFilter();
    get.setFilter(filter);

    Result result = fetchResult(get, rowReader.getTableReader(), graphConfig);
    Map<Long, Map<String, KeyValue>> buckets = buketizeResult(result, graphConfig);

    Set<Long> sequences = new HashSet<Long>();
    // filter sequence results using edge recognizer
    // if (!multiDescendantProperties) {
    // assemble a recognizer once for
    // all results. Then only evaluate each result.
    LocalEdgeRecognizerSyntaxTreeAssembler assembler = new LocalEdgeRecognizerSyntaxTreeAssembler(
        where, graphConfig, contextType, rootType);
    Expr recogniser = assembler.getResult();
    LocalEdgeRecognizerContext context = new LocalEdgeRecognizerContext();
    for (Long seq : buckets.keySet()) {
      Map<String, KeyValue> seqMap = buckets.get(seq);
      context.setSequence(seq);
      context.setKeyMap(seqMap);
      if (recogniser.evaluate(context))
        sequences.add(seq);
    }
    return sequences;
  }

  /**
   * Runs the given get and returns the result.
   * 
   * @param get
   *          the row get
   * @return the result.
   * @throws IOException
   */
  public Result fetchResult(Get get, TableOperation tableOperation, DataGraphConfig graphConfig)
      throws IOException {
    if (log.isDebugEnabled())
      try {
        log.debug("get filter: " + FilterUtil.printFilterTree(get.getFilter()));
      } catch (IOException e1) {
      }

    long before = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("executing get...");

    Result result = tableOperation.getTable().get(get);
    if (result == null) // Note: may not have any key-values
      throw new GraphServiceException("expected result from table "
          + tableOperation.getTableConfig().getName() + " for row '" + new String(get.getRow())
          + "'");

    long after = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("returned 1 results (" + String.valueOf(after - before) + ")");

    return result;
  }

  /**
   * Runs the given get and returns the result.
   * 
   * @param gets
   *          the list of row get operations
   * @return the result.
   * @throws IOException
   */
  public Result[] fetchResult(List<Get> gets, TableOperation tableOperation,
      DataGraphConfig graphConfig) throws IOException {
    if (log.isDebugEnabled())
      try {
        log.debug("get filter: " + FilterUtil.printFilterTree(gets.get(0).getFilter()));
      } catch (IOException e1) {
      }

    long before = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("executing " + gets.size() + " gets...");

    Result[] result = tableOperation.getTable().get(gets);

    long after = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("returned " + result.length + " results (" + String.valueOf(after - before) + ")");

    return result;
  }

  public Map<Long, Map<String, KeyValue>> buketizeResult(Result result, DataGraphConfig graphConfig) {
    Map<Long, Map<String, KeyValue>> resultMap = new HashMap<Long, Map<String, KeyValue>>();

    if (!result.isEmpty())
      for (KeyValue keyValue : result.list()) {
        // FIXME: no parsing here !!
        String qual = Bytes.toString(keyValue.getQualifier());
        if (log.isDebugEnabled())
          log.debug("\tkey: " + qual + "\tvalue: " + Bytes.toString(keyValue.getValue()));
        String[] sections = qual.split(graphConfig.getColumnKeySequenceDelimiter());
        Long seq = Long.valueOf(sections[1]);
        Map<String, KeyValue> subMap = resultMap.get(seq);
        if (subMap == null) {
          subMap = new HashMap<String, KeyValue>();
          resultMap.put(seq, subMap);
        }
        subMap.put(qual, keyValue);
      }

    return resultMap;
  }

  /**
   * Runs the given get and parses the column qualifier sequence number suffixes
   * from the returned columns.
   * 
   * @param get
   *          the row get
   * @return the sequence numbers.
   * @throws IOException
   */
  public Map<Integer, Integer> fetchSequences(Get get, TableReader tableReader,
      DataGraphConfig graphConfig) throws IOException {
    if (log.isDebugEnabled())
      try {
        log.debug("get filter: " + FilterUtil.printFilterTree(get.getFilter()));
      } catch (IOException e1) {
      }

    long before = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("executing get...");

    Result result = tableReader.getTable().get(get);
    if (result == null) // Note: may not have any key-values
      throw new GraphServiceException("expected result from table "
          + tableReader.getTableConfig().getName() + " for row '" + new String(get.getRow()) + "'");

    Map<Integer, Integer> seqMap = new HashMap<Integer, Integer>();
    if (!result.isEmpty())
      for (KeyValue keyValue : result.list()) {
        // FIXME: no parsing here !!
        String qual = Bytes.toString(keyValue.getQualifier());
        if (log.isDebugEnabled())
          log.debug("\tkey: " + qual + "\tvalue: " + Bytes.toString(keyValue.getValue()));
        String[] sections = qual.split(graphConfig.getColumnKeySequenceDelimiter());
        Integer seq = Integer.valueOf(sections[1]);
        seqMap.put(seq, seq);
      }

    long after = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("returned " + seqMap.size() + " results (" + String.valueOf(after - before) + ")");
    return seqMap;
  }

  /**
   * Runs the given scan and parses the column qualifier sequence number
   * suffixes from the returned columns.
   * 
   * @param scan
   *          the row scan
   * @return the sequence numbers.
   */
  public Map<Long, Long> fetchSequences(Scan scan, TableReader tableReader,
      DataGraphConfig graphConfig) {
    if (log.isDebugEnabled())
      try {
        log.debug("scan filter: " + FilterUtil.printFilterTree(scan.getFilter()));
      } catch (IOException e1) {
      }

    if (log.isDebugEnabled())
      log.debug("executing scan...");
    ResultScanner scanner = null;
    try {
      scanner = tableReader.getTable().getScanner(scan);
    } catch (IOException e) {
      throw new GraphServiceException(e);
    }

    Map<Long, Long> result = new HashMap<Long, Long>();
    for (Result row : scanner) {
      if (log.isDebugEnabled())
        log.debug("row: " + new String(row.getRow()));
      for (KeyValue keyValue : row.list()) {
        if (log.isDebugEnabled())
          log.debug("returned " + row.size() + " columns");
        // FIXME: no parsing here !!
        String qual = Bytes.toString(keyValue.getQualifier());
        if (log.isDebugEnabled())
          log.debug("\tkey: " + qual + "\tvalue: " + Bytes.toString(keyValue.getValue()));
        String[] sections = qual.split(graphConfig.getColumnKeySequenceDelimiter());
        Long seq = Long.valueOf(sections[1]);
        result.put(seq, seq);
      }
    }
    return result;
  }

  /**
   * Loads the columns resulting from a
   * {@link BinaryPrefixColumnFilterAssembler} based on the given properties
   * into the given row reader.
   * 
   * @param properties
   *          the properties to fetch
   * @param contextType
   *          the current type
   * @param rowReader
   *          the row reader
   * @throws IOException
   * @see BinaryPrefixColumnFilterAssembler
   */
  public void load(Set<Property> properties, PlasmaType contextType, RowReader rowReader)
      throws IOException {
    Get get = new Get(rowReader.getRowKey());

    PlasmaType rootType = (PlasmaType) rowReader.getRootType();
    BinaryPrefixColumnFilterAssembler columnFilterAssembler = new BinaryPrefixColumnFilterAssembler(
        rootType);
    columnFilterAssembler.assemble(properties, contextType);
    Filter filter = columnFilterAssembler.getFilter();
    get.setFilter(filter);

    load(get, rowReader);
  }

  /**
   * Loads the columns resulting from a
   * {@link StatefullBinaryPrefixColumnFilterAssembler} based on the given
   * properties and the given state sequences into the given row reader.
   * 
   * @param sequences
   *          the sequences
   * @param properties
   *          the properties to fetch
   * @param contextType
   *          the current type
   * @param rowReader
   *          the row reader
   * @throws IOException
   */
  public void loadBySequenceList(Set<Long> sequences, Set<Property> properties,
      PlasmaType contextType, RowReader rowReader, EdgeReader edgeReader) throws IOException {
    Get get = new Get(rowReader.getRowKey());
    PlasmaType rootType = (PlasmaType) rowReader.getRootType();

    StatefullBinaryPrefixColumnFilterAssembler columnFilterAssembler = new StatefullBinaryPrefixColumnFilterAssembler(
        rootType, edgeReader);
    columnFilterAssembler.assemble(properties, sequences, contextType);
    Filter filter = columnFilterAssembler.getFilter();
    get.setFilter(filter);
    load(get, rowReader);
  }

  /**
   * Loads columns returned with the given get and its column filter into the
   * existing row reader.
   * 
   * @param get
   *          the Get operations
   * @param rowReader
   *          the existing row reader
   * @throws IOException
   */
  public void load(Get get, RowReader rowReader) throws IOException {
    if (log.isDebugEnabled())
      try {
        log.debug("get filter: " + FilterUtil.printFilterTree(get.getFilter()));
      } catch (IOException e1) {
      }

    long before = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("executing get...");

    Result result = rowReader.getTableReader().getTable().get(get);
    if (result == null) // do expect a result since a Get oper, but might
      // have no columns
      throw new GraphServiceException("expected result from table "
          + rowReader.getTableReader().getTableConfig().getName() + " for row '"
          + new String(get.getRow()) + "'");
    if (!result.isEmpty())
      for (KeyValue keyValue : result.list()) {
        rowReader.getRow().addColumn(keyValue);
        if (log.isDebugEnabled()) {
          String qual = Bytes.toString(keyValue.getQualifier());
          log.debug("\tkey: " + qual + "\tvalue: " + Bytes.toString(keyValue.getValue()));
        }
      }

    long after = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("returned 1 results (" + String.valueOf(after - before) + ")");
  }

  public void load(Scan scan, RowReader rowReader) {
    if (log.isDebugEnabled())
      try {
        log.debug("scan filter: " + FilterUtil.printFilterTree(scan.getFilter()));
      } catch (IOException e1) {
      }

    if (log.isDebugEnabled())
      log.debug("executing scan...");
    ResultScanner scanner = null;
    try {
      scanner = rowReader.getTableReader().getTable().getScanner(scan);
    } catch (IOException e) {
      throw new GraphServiceException(e);
    }
    for (Result row : scanner) {
      if (log.isDebugEnabled())
        log.debug("row: " + new String(row.getRow()));
      if (log.isDebugEnabled())
        log.debug("returned " + row.size() + " columns");
      for (KeyValue keyValue : row.list()) {
        rowReader.getRow().addColumn(keyValue);
        if (log.isDebugEnabled()) {
          String qual = Bytes.toString(keyValue.getQualifier());
          log.debug("\tkey: " + qual + "\tvalue: " + Bytes.toString(keyValue.getValue()));
        }
      }
    }
  }

  private String serializeGraph(commonj.sdo.DataGraph graph) throws IOException {
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

  protected void log(Where predicates) {
    String xml = "";
    PlasmaQueryDataBinding binding;
    try {
      binding = new PlasmaQueryDataBinding(new DefaultValidationEventHandler());
      xml = binding.marshal(predicates);
    } catch (JAXBException e) {
      log.debug(e);
    } catch (SAXException e) {
      log.debug(e);
    }
    log.debug("query: " + xml);
  }
}
