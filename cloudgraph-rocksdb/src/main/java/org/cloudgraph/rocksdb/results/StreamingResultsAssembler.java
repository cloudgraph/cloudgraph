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
package org.cloudgraph.rocksdb.results;

import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.recognizer.GraphRecognizerContext;
import org.cloudgraph.rocksdb.ext.Result;
import org.cloudgraph.rocksdb.graph.HBaseGraphAssembler;
import org.cloudgraph.rocksdb.io.CellValues;
import org.cloudgraph.rocksdb.io.TableReader;
import org.cloudgraph.store.key.GraphMetaKey;
import org.plasma.sdo.PlasmaDataGraph;

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
public class StreamingResultsAssembler extends DefaultResultsAssembler implements ResultsAssembler {
  private static final Log log = LogFactory.getLog(StreamingResultsAssembler.class);
  protected HBaseGraphAssembler graphAssembler;
  protected PlasmaDataGraph graph;

  public StreamingResultsAssembler(Expr graphRecognizerRootExpr,
      Comparator<PlasmaDataGraph> orderingComparator, TableReader rootTableReader,
      HBaseGraphAssembler graphAssembler, Integer startRange, Integer endRange) {
    super(graphRecognizerRootExpr, orderingComparator, rootTableReader, startRange, endRange);
    this.graphAssembler = graphAssembler;
  }

  @Override
  public boolean collect(Result resultRow) throws IOException {
    if (resultRow.containsColumn(rootTableReader.getTableConfig().getDataColumnFamilyNameBytes(),
        GraphMetaKey.TOMBSTONE.codeAsBytes())) {
      return false; // ignore toumbstone roots
    }

    if (canIgnoreResults() && currentResultIgnored()) {
      return false;
    }

    this.graphAssembler.assemble(new CellValues(resultRow));
    PlasmaDataGraph graph = this.graphAssembler.getDataGraph();
    this.graphAssembler.clear();

    if (this.whereSyntaxTree != null) {
      if (this.whereContext == null)
        this.whereContext = new GraphRecognizerContext();
      this.whereContext.setGraph(graph);
      if (!this.whereSyntaxTree.evaluate(this.whereContext)) {
        if (log.isDebugEnabled())
          log.debug("recognizer excluded: " + Bytes.toString(resultRow.getRow()));
        if (log.isDebugEnabled())
          log.debug(serializeGraph(graph));
        this.unrecognizedResults++;
        return false;
      }
    }

    this.graph = graph;

    return true;
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public PlasmaDataGraph[] getResults() {
    throw new IllegalStateException("not a results collector assembler");
  }

  @Override
  public PlasmaDataGraph getCurrentResult() {
    return this.graph;
  }

}
