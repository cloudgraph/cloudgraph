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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.graph.HBaseGraphAssembler;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.recognizer.GraphRecognizerContext;
import org.cloudgraph.store.key.GraphMetaKey;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.helper.PlasmaXMLHelper;
import org.plasma.sdo.xml.DefaultOptions;

import commonj.sdo.helper.XMLDocument;

/**
 * Assembler which determines whether results can be ignored under the current
 * context then "slides" past results not within the given range, avoiding the
 * overhead of assembling a graph. When a graph recognizer is present for the
 * current context, no result can be bypassed as the recognizer requires an
 * assembled graph to evaluate and make its determination.
 * 
 * @author Scott Cinnamond
 * @since 0.5.9
 * @see HBaseGraphAssembler
 * @see Expr
 * @see GraphRecognizerContext
 */
public class SlidingResultsAssembler implements ResultsAssembler {
  private static final Log log = LogFactory.getLog(SlidingResultsAssembler.class);
  private Set<PlasmaDataGraph> graphs = new HashSet<PlasmaDataGraph>();

  private Expr graphRecognizerRootExpr;
  private GraphRecognizerContext recognizerContext;
  private Comparator<PlasmaDataGraph> orderingComparator;
  private TableReader rootTableReader;
  private HBaseGraphAssembler graphAssembler;
  private Integer startRange;
  private Integer endRange;
  private boolean resultEndRangeReached = false;
  private int unrecognizedRsults;
  private int ignoredResults;
  private int ignoredResultsPreceedingRange;
  private int range;

  private SlidingResultsAssembler() {
  }

  public SlidingResultsAssembler(Expr graphRecognizerRootExpr,
      Comparator<PlasmaDataGraph> orderingComparator, TableReader rootTableReader,
      HBaseGraphAssembler graphAssembler, Integer startRange, Integer endRange) {
    this();
    this.graphRecognizerRootExpr = graphRecognizerRootExpr;
    this.orderingComparator = orderingComparator;
    this.rootTableReader = rootTableReader;
    this.graphAssembler = graphAssembler;
    this.startRange = startRange;
    this.endRange = endRange;
    if (startRange != null && endRange != null) {
      range = endRange.intValue() - startRange.intValue();
      range++; // inclusive
    }
  }

  public boolean collect(Result resultRow) throws IOException {
    if (resultRow.containsColumn(rootTableReader.getTableConfig().getDataColumnFamilyNameBytes(),
        GraphMetaKey.TOMBSTONE.codeAsBytes())) {
      return false; // ignore toumbstone roots
    }

    if (canIgnoreResults()) {
      if (startRange != null && endRange != null) {
        int current = ignoredResultsPreceedingRange + 1;
        if (current < startRange.intValue()) {
          ignoredResultsPreceedingRange++;
          ignoredResults++;
          return false;
        }

        current = this.graphs.size() + 1;
        if (current > range) {
          ignoredResults++;
          this.resultEndRangeReached = true;
          return false;
        }
      }
    }

    this.graphAssembler.assemble(resultRow);
    PlasmaDataGraph graph = this.graphAssembler.getDataGraph();
    this.graphAssembler.clear();

    if (this.graphRecognizerRootExpr != null) {
      if (this.recognizerContext == null)
        this.recognizerContext = new GraphRecognizerContext();
      this.recognizerContext.setGraph(graph);
      if (!this.graphRecognizerRootExpr.evaluate(this.recognizerContext)) {
        if (log.isDebugEnabled())
          log.debug("recognizer excluded: " + Bytes.toString(resultRow.getRow()));
        if (log.isDebugEnabled())
          log.debug(serializeGraph(graph));
        this.unrecognizedRsults++;
        return false;
      }
    }
    this.graphs.add(graph);

    return true;
  }

  @Override
  public int getUnrecognizedResults() {
    return unrecognizedRsults;
  }

  @Override
  public int getIgnoredResults() {
    return ignoredResults;
  }

  @Override
  public boolean isResultEndRangeReached() {
    return resultEndRangeReached;
  }

  @Override
  public PlasmaDataGraph[] getResults() {
    PlasmaDataGraph[] array = new PlasmaDataGraph[graphs.size()];
    this.graphs.toArray(array);
    if (this.orderingComparator != null)
      Arrays.sort(array, this.orderingComparator);
    return array;
  }

  @Override
  public boolean canIgnoreResults() {
    return this.graphRecognizerRootExpr == null;
  }

  @Override
  public int size() {
    return this.graphs.size();
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

}
