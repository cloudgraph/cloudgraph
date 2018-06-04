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
package org.cloudgraph.hbase.results;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.recognizer.GraphRecognizerContext;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.helper.PlasmaXMLHelper;
import org.plasma.sdo.xml.DefaultOptions;

import commonj.sdo.helper.XMLDocument;

/**
 * Default functionality for various results assemblers.
 * 
 * @author Scott Cinnamond
 * @since 1.0.7
 * @see Expr
 * @see GraphRecognizerContext
 */
public abstract class DefaultResultsAssembler implements ResultsAssembler {
  private static final Log log = LogFactory.getLog(DefaultResultsAssembler.class);

  protected Expr whereSyntaxTree;
  protected GraphRecognizerContext whereContext;
  protected Comparator<PlasmaDataGraph> orderingComparator;
  protected TableReader rootTableReader;
  protected Integer startRange;
  protected Integer endRange;
  protected boolean resultEndRangeReached = false;
  protected int unrecognizedResults;
  protected int ignoredResults;
  protected int ignoredResultsPreceedingRange;
  protected int range;

  private DefaultResultsAssembler() {
  }

  public DefaultResultsAssembler(Expr whereSyntaxTree,
      Comparator<PlasmaDataGraph> orderingComparator, TableReader rootTableReader,
      Integer startRange, Integer endRange) {
    this();
    this.whereSyntaxTree = whereSyntaxTree;
    this.orderingComparator = orderingComparator;
    this.rootTableReader = rootTableReader;
    this.startRange = startRange;
    this.endRange = endRange;
    if (startRange != null && endRange != null) {
      range = endRange.intValue() - startRange.intValue();
      range++; // inclusive
    }
  }

  @Override
  public int getUnrecognizedResults() {
    return unrecognizedResults;
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
  public boolean canIgnoreResults() {
    // Cannot ever ignore results if there's a recognizer
    // because it could recognize records across a discontinuous range
    // then, when the client requests the next range/chunk
    // we would not know where to skip to.
    // This could be addressed by storing state
    // somehow on the client. I.e. a skip count or virtual
    // seek index into the records set.
    return this.whereSyntaxTree == null;
  }

  @Override
  public boolean currentResultIgnored() {
    if (canIgnoreResults()) {
      if (startRange != null && endRange != null) {
        int current = ignoredResultsPreceedingRange + 1;
        if (current < startRange.intValue()) {
          ignoredResultsPreceedingRange++;
          ignoredResults++;
          return true;
        }

        current = size() + 1;
        if (current > range) {
          ignoredResults++;
          this.resultEndRangeReached = true;
          return true;
        }
      }
    }
    return false;
  }

  protected String serializeGraph(commonj.sdo.DataGraph graph) {
    DefaultOptions options = new DefaultOptions(graph.getRootObject().getType().getURI());
    options.setRootNamespacePrefix("debug");

    XMLDocument doc = PlasmaXMLHelper.INSTANCE.createDocument(graph.getRootObject(), graph
        .getRootObject().getType().getURI(), null);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      PlasmaXMLHelper.INSTANCE.save(doc, os, options);
      os.flush();
      os.close();
    } catch (IOException e) {
      log.error(e.getMessage(), e); // only used for debugging
    }
    String xml = new String(os.toByteArray());
    return xml;
  }

}
