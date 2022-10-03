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
package org.cloudgraph.rocksdb.graph;

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
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
//import org.cloudgraph.hbase.util.FilterUtil;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprPrinter;
import org.cloudgraph.recognizer.Endpoint;
import org.cloudgraph.recognizer.GraphRecognizerContext;
import org.cloudgraph.recognizer.GraphRecognizerSyntaxTreeAssembler;
import org.cloudgraph.rocksdb.connect.Connection;
import org.cloudgraph.rocksdb.ext.Get;
import org.cloudgraph.rocksdb.ext.KeyInfo;
import org.cloudgraph.rocksdb.ext.Result;
import org.cloudgraph.rocksdb.ext.Scan;
import org.cloudgraph.rocksdb.filter.ColumnInfo;
import org.cloudgraph.rocksdb.filter.MultiColumnCompareFilter;
import org.cloudgraph.rocksdb.filter.RocksDBFilterAssembler;
import org.cloudgraph.rocksdb.filter.BinaryPrefixColumnFilterAssembler;
import org.cloudgraph.rocksdb.filter.ColumnPredicateFilterAssembler;
import org.cloudgraph.rocksdb.filter.Filter;
import org.cloudgraph.rocksdb.filter.GraphFetchColumnFilterAssembler;
import org.cloudgraph.rocksdb.filter.PredicateFilterAssembler;
import org.cloudgraph.rocksdb.filter.PredicateUtil;
import org.cloudgraph.rocksdb.filter.StatefullBinaryPrefixColumnFilterAssembler;
import org.cloudgraph.rocksdb.io.CellValues;
import org.cloudgraph.rocksdb.io.DistributedGraphReader;
import org.cloudgraph.rocksdb.io.DistributedReader;
import org.cloudgraph.rocksdb.io.EdgeReader;
import org.cloudgraph.rocksdb.io.GraphEdgeReader;
import org.cloudgraph.rocksdb.io.KeyValue;
import org.cloudgraph.rocksdb.io.RowReader;
import org.cloudgraph.rocksdb.io.TableOperation;
import org.cloudgraph.rocksdb.io.TableReader;
import org.cloudgraph.rocksdb.io.DefaultEdgeOperation.KeyBytes;
import org.cloudgraph.rocksdb.key.StatefullColumnKeyFactory;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.collector.Selection;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.Where;
import org.plasma.sdo.DataType;
//import org.plasma.sdo.Key;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.helper.PlasmaXMLHelper;
import org.plasma.sdo.profile.KeyStructure;
import org.plasma.sdo.xml.DefaultOptions;
import org.rocksdb.RocksDBException;
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
  private Connection connection;
  private StoreMappingContext mappingContext;

  @SuppressWarnings("unused")
  private GraphSliceSupport() {
  }

  public GraphSliceSupport(Selection selection, Timestamp snapshotDate, Connection connection,
      StoreMappingContext mappingContext) {
    this.selection = selection;
    this.snapshotDate = snapshotDate;
    this.connection = connection;
    this.mappingContext = mappingContext;
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
  public List<CellValues> filter(PlasmaType contextType, int level, EdgeReader edgeReader,
      Where where, RowReader rowReader, TableReader tableReader) throws IOException {
    List<CellValues> results = new ArrayList<>((int) edgeReader.getCount());

    if (!edgeReader.isExternal())
      throw new IllegalStateException("expected external edge collection not, " + edgeReader);

    if (log.isDebugEnabled())
      log(where);
    List<byte[]> graphEvalRowKeys = new ArrayList<>();
    ExternalEdgeRecognizerSyntaxTreeAssembler edgeSyntaxAssembler = new ExternalEdgeRecognizerSyntaxTreeAssembler(
        where, rowReader.getDataGraph(), contextType, rowReader.getRootType());
    Expr edgeRecognizerRootExpr = edgeSyntaxAssembler.getResult();
    if (log.isDebugEnabled()) {
      ExprPrinter printer = new ExprPrinter();
      edgeRecognizerRootExpr.accept(printer);
      log.debug("Edge Recognizer: " + printer.toString());
    }
    CellConverter cellConverter = new CellConverter(contextType, tableReader.getTableConfig(),
        this.mappingContext);
    ExternalEdgeRecognizerContext edgeRecogniserContext = new ExternalEdgeRecognizerContext(
        contextType, this.mappingContext);
    boolean complete = rowKeyModelCompleteSelection(contextType, level,
        edgeRecogniserContext.getEndpoints());
    for (KeyBytes rowKeyBytes : edgeReader.getRowKeys()) {
      edgeRecogniserContext.read(rowKeyBytes.getKey());
      if (edgeRecognizerRootExpr.evaluate(edgeRecogniserContext)) {
        if (!edgeRecogniserContext.isRowEvaluatedCompletely()) {
          graphEvalRowKeys.add(rowKeyBytes.getKey());
        } else {
          CellValues cellValues = cellConverter.convert(rowKeyBytes.getKey(),
              edgeRecogniserContext.getValues());
          cellValues.setCompleteSelection(complete);
          results.add(cellValues);
        }
      }
    }
    if (log.isDebugEnabled())
      log.debug("recognized " + String.valueOf(graphEvalRowKeys.size() + results.size())
          + " out of " + edgeReader.getRowKeys().size() + " external edges");
    if (graphEvalRowKeys.size() == 0)
      return results; // no edges recognized or all recognized by row key alone

    SelectionCollector predicateSelection = new SelectionCollector(where, contextType);

    // create a new reader as the existing one may have cached data objects
    // already
    // linked to the parent graph. Cannot link to sub-graph as well.
    DistributedReader existingReader = (DistributedReader) tableReader.getDistributedOperation();
    DistributedGraphReader sliceGraphReader = new DistributedGraphReader(contextType,
        predicateSelection.getTypes(), existingReader.getConnection(), this.mappingContext);
    // Note: don't need to close this distributed reader because the slice graph
    // must
    // always have a parent graph, which will close the resources

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
    RocksDBFilterAssembler columnFilterAssembler = new GraphFetchColumnFilterAssembler(
        predicateSelection, contextType, this.mappingContext); // use predicate
                                                               // selection not
                                                               // entire
    // graph selection
    Filter columnFilter = columnFilterAssembler.getFilter();

    List<Get> gets = new ArrayList<Get>();
    for (byte[] rowKey : graphEvalRowKeys) {
      // byte[] childRowKey =
      // rowReader.getGraphState().getRowKey(edge.getUuid()); // use local
      // edge UUID
      Get get = new Get(rowKey);
      get.setColumnFilter(columnFilter);
      gets.add(get);
    }
    DataGraphMapping graphConfig = StoreMapping.getInstance().getDataGraph(
        contextType.getQualifiedName(), this.mappingContext);
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
      } else {
        if (log.isDebugEnabled())
          log.debug("recognizer passed: " + Bytes.toString(resultRow.getRow()));
        if (log.isDebugEnabled())
          log.debug(serializeGraph(assembledGraph));
      }

      CellValues values = new CellValues(resultRow.getRow());
      values.setCompleteSelection(false);
      results.add(values);
      rowIndex++;
    }

    return results;
  }

  /**
   * Return true if the row key model represented by the given {@link Endpoint
   * endpoints} represents a complete selection such that a completed data
   * object may be constructed entirely from each row key. This is hugely
   * imports in avoiding data store round trips.
   * 
   * <p>
   * </p>
   * NOTE: This methods never returns true if a root UUID property is not
   * present for the given (row key) {@link Endpoint endpoints}. Every data
   * object must have a UUID, so in order to construct a completed data object
   * entirely from a row key, the row key model MOST contain an {@link Endpoint
   * endpoint} at root level, i.e. level 0 with no path in the row key field
   * definition.
   * 
   * @param contextType
   * @param endpoints
   * @return
   */
  private boolean rowKeyModelCompleteSelection(PlasmaType contextType, int level,
      Collection<Endpoint> endpoints) {

    if (!hasRootUuid(endpoints, contextType))
      return false;

    Set<Property> selectionProps = this.selection.getProperties(contextType, level + 1);
    for (Property prop : selectionProps) {
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
   * Returns true if an {@link Endpoint endpoint} property at root level is
   * found with a key structure indicating a UUID.
   * 
   * @param endpoints
   *          the {@link Endpoint endpoints}
   * @param contextType
   *          the context type
   * @return true if an {@link Endpoint endpoint} property at root level is
   *         found with a key structure indicating a UUID.
   */
  private boolean hasRootUuid(Collection<Endpoint> endpoints, PlasmaType contextType) {
    boolean result = false;
    for (Endpoint endpoint : endpoints) {
      if (endpoint.getLevel() > 0)
        continue; // can't be a root UUID if EP not at level 0
      org.plasma.sdo.Key key = endpoint.getProperty().getKey();
      if (key != null && key.getStructure() != null) {
        KeyStructure keyStruct = KeyStructure.valueOf(key.getStructure().name());
        if (keyStruct.ordinal() == KeyStructure.uuid.ordinal()) {
          PlasmaType endpointOwnerType = (PlasmaType) endpoint.getProperty().getContainingType();
          if (endpointOwnerType.equals(contextType) || contextType.isBaseType(endpointOwnerType)) {
            result = true;
            break;
          }
        }
      }
    }
    return result;
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
  public Set<Long> fetchSequences(PlasmaType contextType, Where where, RowReader rowReader,
      EdgeReader edgeReader) throws IOException {

    if (log.isDebugEnabled()) {
      log.debug("root type: " + rowReader.getRootType());
      log.debug("context type: " + contextType);
      log.debug("fetch sequences: " + this.marshal(where));
    }
    PlasmaType rootType = (PlasmaType) rowReader.getRootType();
    DataGraphMapping graphConfig = StoreMapping.getInstance().getDataGraph(
        rootType.getQualifiedName(), this.mappingContext);
    Get get = new Get(rowReader.getRowKey());

    PredicateUtil predicateUtil = new PredicateUtil();
    PredicateFilterAssembler filterAssembler = null;
    boolean multiDescendantProperties = predicateUtil.hasHeterogeneousDescendantProperties(where);
    // if (!multiDescendantProperties) {
    filterAssembler = new ColumnPredicateFilterAssembler(rootType, this.mappingContext);
    // }
    // else {
    // filterAssembler = new
    // MultiColumnPredicateFilterAssembler(rowReader.getGraphState(),
    // rootType);
    // }
    filterAssembler.assemble(where, contextType);
    Filter filter = filterAssembler.getFilter();
    get.setColumnFilter(filter);

    Result result = fetchResult(get, rowReader.getTableReader(), graphConfig);
    Map<Long, Map<String, KeyValue>> buckets = buketizeResult(result, graphConfig);
    if (log.isDebugEnabled())
      log.debug("found " + buckets.size() + " sequence buckets");

    Set<Long> sequences = new HashSet<Long>();
    // filter sequence results using edge recognizer
    // if (!multiDescendantProperties) {
    // assemble a recognizer once for
    // all results. Then only evaluate each result.
    LocalEdgeRecognizerSyntaxTreeAssembler assembler = new LocalEdgeRecognizerSyntaxTreeAssembler(
        where, graphConfig, contextType, rootType, this.mappingContext);
    Expr recogniser = assembler.getResult();
    GraphEdgeReader ger = (GraphEdgeReader) edgeReader;

    LocalEdgeRecognizerContext context = new LocalEdgeRecognizerContext(contextType,
        ger.getSourceProp());
    for (Long seq : buckets.keySet()) {
      // possible for returned sequence no NOT be part of edge
      if (edgeReader.hasSequence(seq)) {
        Map<String, KeyValue> seqMap = buckets.get(seq);
        context.setSequence(seq);
        context.setKeyMap(seqMap);
        if (recogniser.evaluate(context))
          sequences.add(seq);
      }
    }
    if (log.isDebugEnabled())
      log.debug("returning " + sequences.size() + " sequences");
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
  public Result fetchResult(Get get, TableOperation tableOperation, DataGraphMapping graphConfig)
      throws IOException {

    long before = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("executing get...");

    Result result = tableOperation.getTable().get(get);
    if (result == null) // Note: may not have any key-values
      throw new GraphServiceException("expected result from table "
          + tableOperation.getTableConfig().getQualifiedPhysicalName() + " for row '"
          + new String(get.getRow()) + "'");

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
      DataGraphMapping graphConfig) throws IOException {

    long before = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("executing " + gets.size() + " gets...");

    Result[] result = tableOperation.getTable().get(gets);

    long after = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("returned " + result.length + " results (" + String.valueOf(after - before) + ")");

    return result;
  }

  public Map<Long, Map<String, KeyValue>> buketizeResult(Result result, DataGraphMapping graphConfig) {
    Map<Long, Map<String, KeyValue>> resultMap = new HashMap<Long, Map<String, KeyValue>>();

    if (!result.isEmpty())
      for (KeyValue keyValue : result.list()) {
        // FIXME: no parsing here !!
        String qual = Bytes.toString(keyValue.getQualifier());
        if (log.isDebugEnabled())
          log.debug("\tkey: " + qual + "\tvalue: " + Bytes.toString(keyValue.getValue()));
        String[] sections = qual.split(graphConfig.getColumnKeySequenceDelimiter());
        if (sections.length < 2) {
          log.warn("ignoring non statefull qualifier '"
              + qual
              + "' - can be caused by multiple physical property names for multiple entities within the same (bound) data graph");
          continue;
        }
        Long seq = 0L;
        try {
          seq = Long.valueOf(sections[1]);
        } catch (NumberFormatException e) {
          int foo = 0;
          foo++;
          log.warn("ignoring qualifier: ' " + qual + "'");
          continue;
        }
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
      DataGraphMapping graphConfig) throws IOException {
    long before = System.currentTimeMillis();
    if (log.isDebugEnabled())
      log.debug("executing get...");

    Result result = tableReader.getTable().get(get);
    if (result == null) // Note: may not have any key-values
      throw new GraphServiceException("expected result from table "
          + tableReader.getTableConfig().getQualifiedPhysicalName() + " for row '"
          + new String(get.getRow()) + "'");

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
  public Map<Long, Long> fetchSequences(Scan scan, TableReader tableReader, RowReader rowReader,
      DataGraphMapping graphConfig) {

    Map<Long, Long> result = new HashMap<Long, Long>();
    if (log.isDebugEnabled())
      log.debug("executing scan...");

    Result[] recordSet = tableReader.getTable().scan(scan);
    for (Result resultRow : recordSet) {
      if (log.isDebugEnabled())
        log.debug("row: " + resultRow.toString());
      // Key key = new Key(tableReader.getTable().getNamespace(),
      // tableReader.getTable().getSetName(),
      // rowReader.getRowKey());
      // KeyInfo ki = new KeyInfo(key,
      // tableReader.getTableConfig().getDataColumnFamilyName());
      // Result resultRow = new Result(ki, row, scan.getColumnFilter());

      for (KeyValue keyValue : resultRow.list()) {
        if (log.isDebugEnabled())
          log.debug("returned " + resultRow.size() + " columns");
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
        rootType, this.mappingContext);
    columnFilterAssembler.assemble(properties, contextType);
    Filter filter = columnFilterAssembler.getFilter();
    get.setColumnFilter(filter);

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
    if (anyReferenceProperties(properties)) {
      StatefullBinaryPrefixColumnFilterAssembler columnFilterAssembler = new StatefullBinaryPrefixColumnFilterAssembler(
          rootType, edgeReader);
      columnFilterAssembler.assemble(properties, sequences, contextType);
      Filter filter = columnFilterAssembler.getFilter();
      get.setColumnFilter(filter);
    } else {
      StatefullColumnKeyFactory columnKeyFac = new StatefullColumnKeyFactory(rootType,
          this.mappingContext);
      PlasmaType subType = edgeReader.getSubType();
      if (subType == null)
        subType = edgeReader.getBaseType();
      byte[] colFam = columnKeyFac.getGraph().getTable().getDataColumnFamilyNameBytes();
      byte[] colKey = null;
      Map<String, ColumnInfo> map = new HashMap<>();
      for (Long seq : sequences) {
        for (EntityMetaKey metaField : EntityMetaKey.values()) {
          colKey = columnKeyFac.createColumnKey(subType, seq, metaField);
          // get.addColumn(colFam, colKey, metaField.getStorageType());
          ColumnInfo ci = new ColumnInfo(colFam, colKey, metaField.getStorageType());
          map.put(ci.getColumn(), ci);
        }
        for (Property p : properties) {
          PlasmaProperty prop = (PlasmaProperty) p;
          colKey = columnKeyFac.createColumnKey(subType, seq, prop);
          DataType propDataType = DataType.valueOf(prop.getType().getName());
          // get.addColumn(colFam, colKey, propDataType);
          ColumnInfo ci = new ColumnInfo(colFam, colKey, propDataType);
          map.put(ci.getColumn(), ci);
        }
      }
      get.setColumnFilter(new MultiColumnCompareFilter(map));
    }
    load(get, rowReader);
  }

  private boolean anyReferenceProperties(Set<Property> properties) {
    for (Property p : properties) {
      if (!p.getType().isDataType())
        return true;
    }
    return false;
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
        if (get.getColumnFilter() != null) {
          log.debug("executing get: " + get.getColumnFilter());
        } else {
          StringBuilder buf = new StringBuilder();
          buf.append("columns: [");
          Map<byte[], NavigableSet<byte[]>> fm = null; // FIXME:
                                                       // get.getFamilyMap();
          int i = 0;
          for (Map.Entry<byte[], NavigableSet<byte[]>> entry : fm.entrySet()) {
            for (byte[] column : entry.getValue()) {
              if (i > 0)
                buf.append(" ");
              buf.append(Bytes.toString(entry.getKey()));
              buf.append(":");
              buf.append(Bytes.toString(column));
            }
            i++;
          }
          buf.append("] count: " + i);
          log.debug("executing get: " + buf.toString());
        }
      } catch (Exception e1) {
      }

    long before = System.currentTimeMillis();
    Result result = rowReader.getTableReader().getTable().get(get);
    long after = System.currentTimeMillis();

    if (result == null) // do expect a result since a Get oper, but might
      // have no columns
      throw new GraphServiceException("expected result from table "
          + rowReader.getTableReader().getTableConfig().getQualifiedPhysicalName() + " for row '"
          + new String(get.getRow()) + "'");
    if (!result.isEmpty())
      for (KeyValue keyValue : result.list()) {
        rowReader.getRow().addColumn(keyValue);
        if (log.isDebugEnabled()) {
          String qual = Bytes.toString(keyValue.getQualifier());
          log.debug("\tkey: " + qual + "\tvalue: " + Bytes.toString(keyValue.getValue()));
        }
      }

    if (log.isDebugEnabled())
      log.debug("returned 1 results (" + String.valueOf(after - before) + ")");
  }

  public void load(Scan scan, RowReader rowReader) {

    Result[] recordSet = rowReader.getTableReader().getTable().scan(scan);
    for (Result resultRow : recordSet) {
      // Record row = recordSet.getRecord();
      // if (log.isDebugEnabled())
      // log.debug("row: " + row.toString());
      // Key key = new Key(rowReader.getTableReader().getTable().getNamespace(),
      // rowReader
      // .getTableReader().getTable().getSetName(), rowReader.getRowKey());
      // KeyInfo ki = new KeyInfo(key,
      // rowReader.getTableReader().getTableConfig()
      // .getDataColumnFamilyName());
      // Result resultRow = new Result(ki, row, scan.getColumnFilter());
      if (log.isDebugEnabled())
        log.debug("returned " + resultRow.size() + " columns");
      for (KeyValue keyValue : resultRow.list()) {
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
    log.debug("query: " + marshal(predicates));
  }

  protected String marshal(Where predicates) {
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
    return xml;
  }
}
