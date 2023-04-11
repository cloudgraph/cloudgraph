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
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import jakarta.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.client.Table;
//import org.cloudgraph.core.client.Result;
//import org.cloudgraph.core.client.ResultScanner;
//import org.cloudgraph.core.client.Scan;
//import org.cloudgraph.core.client.Table;

import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.core.Connection;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.CellValues;
import org.cloudgraph.core.graph.CoreGraphAssembler;
import org.cloudgraph.core.graph.GraphAssembler;
import org.cloudgraph.core.graph.GraphSliceAssembler;
import org.cloudgraph.core.io.DistributedGraphReader;
import org.cloudgraph.core.io.DistributedReader;
import org.cloudgraph.core.io.TableReader;
import org.cloudgraph.hbase.connect.HBaseConnectionManager;
import org.cloudgraph.hbase.io.HBaseCellValues;
import org.cloudgraph.hbase.service.HBaseServiceContext;
import org.cloudgraph.mapreduce.Counters;
import org.cloudgraph.mapreduce.GraphWritable;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprPrinter;
import org.cloudgraph.recognizer.GraphRecognizerContext;
import org.cloudgraph.recognizer.GraphRecognizerSyntaxTreeAssembler;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.mapping.CloudGraphStoreMapping;
import org.cloudgraph.store.mapping.ConfigurationProperty;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.DynamicTableMapping;
import org.cloudgraph.store.mapping.MappingConfiguration;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.StoreMappingDataBinding;
import org.cloudgraph.store.mapping.StoreMappingException;
import org.cloudgraph.store.mapping.StoreMappingValidationEventHandler;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.collector.Selection;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.From;
import org.plasma.query.model.Query;
import org.plasma.query.model.Where;
import org.plasma.runtime.PlasmaRuntime;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreDataObject;
import org.plasma.sdo.helper.PlasmaTypeHelper;
import org.xml.sax.SAXException;

import commonj.sdo.Type;

/**
 * Iterates over HBase root table data for the current <code>TableSplit</code>,
 * assembling data graphs based on the detailed selection criteria within a
 * given <a href="http://plasma-sdo.org/org/plasma/query/Query.html">query</a>.
 * Partially or fully assembled data graphs may be passed to binary expression
 * recognizer and potentially screened from client {@link GraphMapper}
 * extensions, potentially eliminating business logic dedicated to identifying
 * specific records.
 * <p>
 * The recognizer uses a binary expression tree assembler which constructs an
 * operator precedence map, then visits (traverses) the given predicate
 * expression syntax tree depth-first using an adapted shunting-yard algorithm
 * and assembles a resulting binary tree structure with expression nodes
 * specific for detecting i.e. recognizing a data graph. In typical usage
 * scenarios, a single expression tree is assembled once, and then used to
 * evaluate (recognize) any number of data graphs.
 * <p>
 * The adapted shunting-yard algorithm in general uses a stack of operators and
 * operands, and as new binary tree nodes are detected and created they are
 * pushed onto the operand stack based on operator precedence. The resulting
 * binary expression tree reflects the syntax of the underlying query expression
 * including the precedence of its operators.
 * </p>
 * </p>
 * 
 * @see GraphRecognizerSyntaxTreeAssembler
 * @see GraphRecognizerContext
 * @see GraphWritable
 * @see CoreGraphAssembler
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public class GraphRecordRecognizer {
  public static final String LOG_PER_ROW_COUNT = "hbase.mapreduce.log.scanner.rowcount";

  static final Log log = LogFactory.getLog(GraphRecordRecognizer.class);

  /** name of mapreduce counter group for HBase */
  private ResultScanner scanner = null;
  private Scan scan = null;
  private Scan currentScan = null;
  private Table table = null;
  private byte[] lastSuccessfulRow = null;
  private ImmutableBytesWritable key = null;
  private GraphWritable value = null;
  private TaskAttemptContext context = null;
  private Method getCounter = null;
  private long numRestarts = 0;
  private long timestamp;
  private int rowcount;
  private boolean logScannerActivity = false;
  private int logPerRowCount = 100;
  private Expr graphRecognizerRootExpr;
  private CoreGraphAssembler graphAssembler;
  private GraphRecognizerContext recognizerContext;
  private TableReader rootTableReader;
  private Connection connection;

  private long numRecognizedGraphs = 0;

  private long numUnrecognizedGraphs = 0;

  private long totalGraphNodesAssembled = 0;
  private long totalGraphAssemblyTime = 0;
  private long totalGrapRecognitionTime = 0;

  private Configuration configuration;
  private ServiceContext serviceContext;
  private StoreMappingContext mappingContext;

  @SuppressWarnings("unused")
  private GraphRecordRecognizer() {
  }

  public GraphRecordRecognizer(Configuration configuration) {
    this.configuration = configuration;
    Properties mappingProps = new Properties();
    String rootPath = this.configuration
        .get(ConfigurationProperty.CLOUDGRAPH___TABLE___NAMESPACE___ROOT.value());
    if (rootPath != null)
      mappingProps.setProperty(ConfigurationProperty.CLOUDGRAPH___TABLE___NAMESPACE___ROOT.value(),
          rootPath);
    String volume = this.configuration.get(ConfigurationProperty.CLOUDGRAPH___TABLE___VOLUME___NAME
        .value());
    if (volume != null)
      mappingProps.setProperty(ConfigurationProperty.CLOUDGRAPH___TABLE___VOLUME___NAME.value(),
          volume);
    this.serviceContext = new HBaseServiceContext(mappingProps);
    this.mappingContext = this.serviceContext.getStoreMapping();
  }

  /**
   * Restart from survivable exceptions by creating a new scanner.
   * 
   * @param firstRow
   *          The first row to start at.
   * @throws IOException
   *           When restarting fails.
   */
  public void restart(byte[] firstRow) throws IOException {
    currentScan = new Scan(scan);
    currentScan.setStartRow(firstRow);
    // currentScan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE,
    // Bytes.toBytes(Boolean.TRUE));

    this.scanner = this.table.getScanner(currentScan);
    if (logScannerActivity) {
      log.info("Current scan=" + currentScan.toString());
      timestamp = System.currentTimeMillis();
      rowcount = 0;
    }
  }

  /**
   * In new mapreduce APIs, TaskAttemptContext has two getCounter methods Check
   * if getCounter(String, String) method is available.
   * 
   * @return The getCounter method or null if not available.
   * @throws IOException
   */
  private Method retrieveGetCounterWithStringsParams(TaskAttemptContext context) throws IOException {
    Method m = null;
    try {
      m = context.getClass().getMethod("getCounter", new Class[] { String.class, String.class });
    } catch (SecurityException e) {
      throw new IOException("Failed test for getCounter", e);
    } catch (NoSuchMethodException e) {
      // Ignore
    }
    return m;
  }

  /**
   * Sets the HBase table.
   * 
   * @param table
   *          The root {@link Table} to scan.
   */
  public void setTable(Table table) {
    Configuration conf = (Configuration) table.getConfiguration();
    logScannerActivity = conf.getBoolean(ScannerCallable.LOG_SCANNER_ACTIVITY, false);
    logPerRowCount = conf.getInt(LOG_PER_ROW_COUNT, 100);
    this.table = table;
  }

  /**
   * Sets the scan defining the actual details like columns etc.
   * 
   * @param scan
   *          The scan to set.
   */
  public void setScan(Scan scan) {
    this.scan = scan;
  }

  /**
   * 
   * Not done in constructor to allow for extension.
   * 
   * @throws IOException
   *           , InterruptedException
   */
  public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException,
      InterruptedException {
    if (context != null) {
      this.context = context;
      this.getCounter = retrieveGetCounterWithStringsParams(context);

      String queryXml = context.getConfiguration().get(GraphInputFormat.QUERY);
      Query query = unmarshal(queryXml);

      String mappingXml = context.getConfiguration().get(GraphInputFormat.TABLE_MAPPINGS);
      if (mappingXml != null) {
        try {
          StoreMappingDataBinding binding = new StoreMappingDataBinding(
              new StoreMappingValidationEventHandler());
          CloudGraphStoreMapping result = (CloudGraphStoreMapping) binding.validate(mappingXml);
          for (org.cloudgraph.store.mapping.Table mapping : result.getTables()) {
            for (org.cloudgraph.store.mapping.DataGraph graph : mapping.getDataGraphs())
              if (!PlasmaRuntime.getInstance().hasSDONamespace(graph.getUri()))
                PlasmaRuntime.getInstance().addDynamicSDONamespace(graph.getUri(), null);
            loadMapping(mapping);
          }
        } catch (JAXBException e) {
          throw new StoreMappingException(e);
        } catch (SAXException e) {
          throw new StoreMappingException(e);
        }
      }

      PlasmaType type = getRootType(query);
      Where where = query.findWhereClause();
      SelectionCollector selectionCollector = null;
      if (where != null)
        selectionCollector = new SelectionCollector(query.getSelectClause(), where, type);
      else
        selectionCollector = new SelectionCollector(query.getSelectClause(), type);
      selectionCollector.setOnlyDeclaredProperties(false);
      // FIXME generalize
      for (Type t : selectionCollector.getTypes())
        collectRowKeyProperties(selectionCollector, (PlasmaType) t, this.mappingContext);

      this.connection = HBaseConnectionManager.instance().getConnection();
      DistributedGraphReader graphReader = new DistributedGraphReader(type,
          selectionCollector.getTypes(), this.connection, this.serviceContext);
      this.rootTableReader = graphReader.getRootTableReader();

      this.graphAssembler = createGraphAssembler(type, this.serviceContext, graphReader,
          selectionCollector, new Timestamp(System.currentTimeMillis()));

      boolean needsRecognizer = context.getConfiguration().getBoolean(GraphInputFormat.RECOGNIZER,
          false);
      if (needsRecognizer) {
        GraphRecognizerSyntaxTreeAssembler recognizerAssembler = new GraphRecognizerSyntaxTreeAssembler(
            where, type);
        this.graphRecognizerRootExpr = recognizerAssembler.getResult();
        this.recognizerContext = new GraphRecognizerContext();
        if (log.isDebugEnabled()) {
          ExprPrinter printer = new ExprPrinter();
          graphRecognizerRootExpr.accept(printer);
          log.debug("Graph Recognizer: " + printer.toString());
        }
      }

    }
    restart(scan.getStartRow());
  }

  private void loadMapping(org.cloudgraph.store.mapping.Table table) {
    TableMapping tableCondig = new DynamicTableMapping(table, this.mappingContext);
    if (StoreMapping.getInstance().findTableByQualifiedLogicalName(
        tableCondig.getQualifiedLogicalName(), this.mappingContext) == null)
      StoreMapping.getInstance().addTable(tableCondig);
  }

  /**
   * Closes the split.
   */
  public void close() {
    try {
      this.connection.close();
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    this.scanner.close();
  }

  /**
   * Returns the current key.
   * 
   * @return The current key.
   * @throws IOException
   * @throws InterruptedException
   *           When the job is aborted.
   */
  public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  /**
   * Returns the current value.
   * 
   * @return The current value.
   * @throws IOException
   *           When the value is faulty.
   * @throws InterruptedException
   *           When the job is aborted.
   */
  public GraphWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  /**
   * Positions the record reader to the next recognized record, scanning forward
   * past any unrecognized records, not passing these records to
   * <code>Mapper</code> clients.
   * 
   * @return <code>true</code> if there was another record.
   * @throws IOException
   *           When reading the record failed.
   * @throws InterruptedException
   *           When the job was aborted.
   */
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (key == null)
      key = new ImmutableBytesWritable();
    Result resultRow = null;
    try {
      try {
        while ((resultRow = this.scanner.next()) != null) {
          PlasmaDataGraph graph = recognize(resultRow);
          if (graph != null) {
            this.value = new GraphWritable(graph);
            break;
          }
        }

        if (logScannerActivity) {
          rowcount++;
          if (rowcount >= logPerRowCount) {
            long now = System.currentTimeMillis();
            log.info("Mapper took " + (now - timestamp) + "ms to process " + rowcount + " rows");
            timestamp = now;
            rowcount = 0;
          }
        }
      } catch (IOException e) {
        // try to handle all IOExceptions by restarting
        // the scanner, if the second call fails, it will be rethrown
        log.info("recovered from " + StringUtils.stringifyException(e));
        if (lastSuccessfulRow == null) {
          log.warn("We are restarting the first next() invocation,"
              + " if your mapper has restarted a few other times like this"
              + " then you should consider killing this job and investigate"
              + " why it's taking so long.");
        }
        if (lastSuccessfulRow == null) {
          restart(scan.getStartRow());
        } else {
          restart(lastSuccessfulRow);
          scanner.next(); // skip presumed already mapped row
        }
        while ((resultRow = this.scanner.next()) != null) {
          PlasmaDataGraph graph = recognize(resultRow);
          if (graph != null) {
            this.value = new GraphWritable(graph);
            break;
          }
        }
        this.numRestarts++;
      }
      if (resultRow != null && resultRow.size() > 0) {
        key.set(resultRow.getRow());
        lastSuccessfulRow = key.get();
        return true;
      }

      updateCounters();
      return false;
    } catch (IOException ioe) {
      if (logScannerActivity) {
        long now = System.currentTimeMillis();
        log.info("Mapper took " + (now - timestamp) + "ms to process " + rowcount + " rows");
        log.info(ioe);
        String lastRow = lastSuccessfulRow == null ? "null" : Bytes
            .toStringBinary(lastSuccessfulRow);
        log.info("lastSuccessfulRow=" + lastRow);
      }
      throw ioe;
    }
  }

  private boolean requiresRecognizer() {
    return this.graphRecognizerRootExpr != null;
  }

  /**
   * Assembles a graph for the given row, then if the current query context
   * requires a graph recognizer, invokes the recognizer returning the graph it
   * it is recognized or null if not.
   * 
   * @param resultRow
   *          the row
   * @return the recognized graph or null if the assembled graph is not
   *         recognized
   */
  private PlasmaDataGraph recognize(Result resultRow) {
    if (resultRow.containsColumn(rootTableReader.getTableConfig().getDataColumnFamilyNameBytes(),
        GraphMetaKey.TOMBSTONE.codeAsBytes())) {
      return null; // ignore toumbstone roots
    }
    PlasmaDataGraph graph = assemble(resultRow);
    if (requiresRecognizer()) {
      long before = System.currentTimeMillis();
      this.recognizerContext.setGraph(graph);
      if (this.graphRecognizerRootExpr.evaluate(recognizerContext)) {
        this.numRecognizedGraphs++;
      } else {
        this.numUnrecognizedGraphs++;
        graph = null;
      }
      long after = System.currentTimeMillis();
      this.totalGrapRecognitionTime += (after - before);
    }

    return graph;
  }

  /**
   * Assembles a data graph from the given result row, capturing various graph
   * metrics and counters.
   * 
   * @param resultRow
   *          the row
   * @return the assembled data graph
   */
  private PlasmaDataGraph assemble(Result resultRow) {
    this.graphAssembler.clear();
    this.graphAssembler.assemble(new HBaseCellValues(resultRow));
    PlasmaDataGraph result = graphAssembler.getDataGraph();
    CoreDataObject root = (CoreDataObject) result.getRootObject();
    Long time = (Long) root.getValue(CloudGraphConstants.GRAPH_ASSEMBLY_TIME);
    if (time != null)
      this.totalGraphAssemblyTime += time.longValue();
    Long nodeCount = (Long) root.getValue(CloudGraphConstants.GRAPH_NODE_COUNT);
    if (nodeCount != null)
      this.totalGraphNodesAssembled += nodeCount.longValue();
    return graphAssembler.getDataGraph();
  }

  /**
   * Updates various job counters.
   * 
   * @throws IOException
   */
  private void updateCounters() throws IOException {
    // we can get access to counters only if hbase uses new mapreduce APIs
    if (this.getCounter == null) {
      return;
    }
    updateHBaseCounters();
    try {
      if (this.numRecognizedGraphs > 0)
        ((Counter) this.getCounter.invoke(context, Counters.CLOUDGRAPH_COUNTER_GROUP_NAME,
            Counters.CLOUDGRAPH_COUNTER_NAME_NUM_RECOGNIZED_GRAPHS))
            .increment(this.numRecognizedGraphs);
      if (this.numUnrecognizedGraphs > 0)
        ((Counter) this.getCounter.invoke(context, Counters.CLOUDGRAPH_COUNTER_GROUP_NAME,
            Counters.CLOUDGRAPH_COUNTER_NAME_NUM_UNRECOGNIZED_GRAPHS))
            .increment(this.numUnrecognizedGraphs);

      ((Counter) this.getCounter.invoke(context, Counters.CLOUDGRAPH_COUNTER_GROUP_NAME,
          Counters.CLOUDGRAPH_COUNTER_NAME_NUM_GRAPH_NODES_ASSEMBLED))
          .increment(this.totalGraphNodesAssembled);

      ((Counter) this.getCounter.invoke(context, Counters.CLOUDGRAPH_COUNTER_GROUP_NAME,
          Counters.CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_ASSEMBLY_TIME))
          .increment(this.totalGraphAssemblyTime);

      ((Counter) this.getCounter.invoke(context, Counters.CLOUDGRAPH_COUNTER_GROUP_NAME,
          Counters.CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_RECOG_TIME))
          .increment(this.totalGrapRecognitionTime);

    } catch (Exception e) {
      log.debug("can't update counter." + StringUtils.stringifyException(e));
    }
  }

  /**
   * Increments various HBase specific counters, mostly taken from
   * <code>ScanMetrics</code>.
   * 
   * @throws IOException
   */
  private void updateHBaseCounters() throws IOException {
    try {
      ((Counter) this.getCounter.invoke(context, HBaseCounters.HBASE_COUNTER_GROUP_NAME,
          HBaseCounters.HBASE_COUNTER_NAME_NUM_SCANNER_RESTARTS)).increment(numRestarts);
    } catch (Exception e) {
      log.debug("can't update counter." + StringUtils.stringifyException(e));
    }
  }

  /**
   * The current progress of the record reader through its data.
   * 
   * @return A number between 0.0 and 1.0, the fraction of the data read.
   */
  public float getProgress() {
    // Depends on the total number of tuples
    return 0;
  }

  private static void collectRowKeyProperties(SelectionCollector collector, PlasmaType type,
      StoreMappingContext mappingContext) {
    MappingConfiguration config = StoreMapping.getInstance();
    DataGraphMapping graph = config.findDataGraph(type.getQualifiedName(), mappingContext);
    if (graph != null) {
      DataRowKeyFieldMapping[] fields = new DataRowKeyFieldMapping[graph
          .getUserDefinedRowKeyFields().size()];
      graph.getUserDefinedRowKeyFields().toArray(fields);
      for (DataRowKeyFieldMapping field : fields) {
        List<Type> types = collector.addProperty(graph.getRootType(), field.getPropertyPath());
        for (Type nextType : types)
          collectRowKeyProperties(collector, (PlasmaType) nextType, mappingContext);
      }
    }
  }

  private static PlasmaType getRootType(Query query) {
    From from = query.getFromClause();
    if (from.getEntity() == null)
      throw new GraphServiceException("given query has no root type and/or URI");
    if (from.getEntity().getName() == null || from.getEntity().getNamespaceURI() == null)
      throw new GraphServiceException("given query has no root type and/or URI");
    String uri = from.getEntity().getNamespaceURI();
    if (!PlasmaRuntime.getInstance().hasSDONamespace(uri))
      PlasmaRuntime.getInstance().addDynamicSDONamespace(uri, null);
    PlasmaType type = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(uri, from.getEntity()
        .getName());
    return type;
  }

  private static CoreGraphAssembler createGraphAssembler(PlasmaType type,
      ServiceContext serviceContext, DistributedReader graphReader, Selection collector,
      Timestamp snapshotDate) {
    CoreGraphAssembler graphAssembler = null;

    if (collector.hasPredicates()) {
      graphAssembler = new GraphSliceAssembler(type, serviceContext, collector, graphReader,
          snapshotDate);
    } else {
      graphAssembler = new GraphAssembler(type, collector, graphReader, snapshotDate);
    }

    return graphAssembler;
  }

  private static Query unmarshal(String xmlQuery) {
    try {
      PlasmaQueryDataBinding binding = new PlasmaQueryDataBinding(
          new DefaultValidationEventHandler());
      return (Query) binding.unmarshal(xmlQuery);
    } catch (JAXBException e1) {
      throw new GraphServiceException(e1);
    } catch (SAXException e1) {
      throw new GraphServiceException(e1);
    }
  }

}
