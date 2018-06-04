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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.graph.HBaseGraphAssembler;
import org.cloudgraph.hbase.io.CellValues;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.recognizer.GraphRecognizerContext;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.collector.FunctionPath;
import org.plasma.query.collector.Selection;
import org.plasma.query.model.FunctionName;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.core.CoreNode;
import org.plasma.sdo.helper.DataConverter;

/**
 * Assembler which executes aggregate functions, grouping and having clause
 * syntax tree evaluation.
 * <p>
 * </p>
 * Determines whether results can be ignored under the current context then
 * "slides" past results not within the given range, avoiding the overhead of
 * assembling a graph.
 * 
 * @author Scott Cinnamond
 * @since 1.4.1
 * @see FunctionPath
 * @see Expr
 * @see GraphRecognizerContext
 */
public class ResultsAggregator extends DefaultResultsAssembler implements ResultsAssembler {
  private static final Log log = LogFactory.getLog(ResultsAggregator.class);
  private static final String PROPERTY_NAME_GROUP_HITS = "groupHits";
  protected HBaseGraphAssembler graphAssembler;
  protected Map<PlasmaDataGraph, PlasmaDataGraph> graphs;
  protected PlasmaDataGraph current;
  protected Selection selection;
  protected List<FunctionPath> funcPaths;
  protected Expr havingSyntaxTree;
  protected GraphRecognizerContext havingContext;
  protected ResultsComparator groupingComparator;

  public ResultsAggregator(Selection selection, Expr whereSyntaxTree,
      ResultsComparator orderingComparator,
      ResultsComparator groupingComparator, Expr havingSyntaxTree,
      TableReader rootTableReader, HBaseGraphAssembler graphAssembler, Integer startRange,
      Integer endRange) {
    super(whereSyntaxTree, orderingComparator, rootTableReader, startRange, endRange);
    this.selection = selection;
    this.graphAssembler = graphAssembler;
    this.havingSyntaxTree = havingSyntaxTree;
    this.graphs = new TreeMap<PlasmaDataGraph, PlasmaDataGraph>(groupingComparator);
    this.groupingComparator = groupingComparator;
    this.funcPaths = this.selection.getAggregateFunctions();
  }

  @Override
  public boolean collect(Result resultRow) throws IOException {
    if (resultRow.containsColumn(rootTableReader.getTableConfig().getDataColumnFamilyNameBytes(),
        GraphMetaKey.TOMBSTONE.codeAsBytes())) {
      return false; // ignore toumbstone roots
    }

    if (canIgnoreResults() && currentResultIgnored()) {
      return false; // slide past it
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
          log.debug("where recognizer excluded: " + Bytes.toString(resultRow.getRow()));
        if (log.isDebugEnabled())
          log.debug(serializeGraph(graph));
        this.unrecognizedResults++;
        return false;
      }
    }

    if (this.graphs.containsKey(graph)) {
      PlasmaDataGraph existing = this.graphs.get(graph);
      CoreNode existingNode = (CoreNode) existing.getRootObject();
      int existingCount = (Integer) existingNode.getValueObject().get(PROPERTY_NAME_GROUP_HITS);
      existingNode.getValueObject().put(PROPERTY_NAME_GROUP_HITS, existingCount + 1);
      for (FunctionPath funcPath : funcPaths) {
        if (!funcPath.getFunc().getName().isAggreate())
          throw new GraphServiceException("expected aggregate function not, "
              + funcPath.getFunc().getName());
        this.aggregate(funcPath, existing, graph);
      }
    } else { // init new aggregate graph
      CoreNode graphNode = (CoreNode) graph.getRootObject();
      graphNode.getValueObject().put(PROPERTY_NAME_GROUP_HITS, 1);
      this.graphs.put(graph, graph);

      for (FunctionPath funcPath : funcPaths) {
        if (!funcPath.getFunc().getName().isAggreate())
          throw new GraphServiceException("expected aggregate function not, "
              + funcPath.getFunc().getName());
        this.initAggregate(funcPath, graph);
      }
    }
    this.current = graph;

    return true;
  }

  private void aggregate(FunctionPath funcPath, PlasmaDataGraph existing, PlasmaDataGraph graph) {
    DataType scalarType = funcPath.getFunc().getName().getScalarDatatype(funcPath.getDataType());

    PlasmaDataObject existingEndpoint = null;
    PlasmaDataObject newEndpoint = null;
    if (funcPath.getPath().size() == 0) {
      existingEndpoint = (PlasmaDataObject) existing.getRootObject();
      newEndpoint = (PlasmaDataObject) graph.getRootObject();
    } else {
      existingEndpoint = (PlasmaDataObject) existing.getRootObject().getDataObject(
          funcPath.getPath().toString());
      newEndpoint = (PlasmaDataObject) graph.getRootObject().getDataObject(
          funcPath.getPath().toString());
    }

    Number existingFuncValue = (Number) existingEndpoint.get(funcPath.getFunc().getName(),
        funcPath.getProperty());
    if (funcPath.getFunc().getName().ordinal() == FunctionName.COUNT.ordinal()) {
      Long newCount = existingFuncValue.longValue() + 1;
      if (newCount > 0) {
        existingEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), newCount);
      } else { // overflow
        log.warn("aggregate " + funcPath.getFunc().getName() + " overflow for target type "
            + scalarType + " - truncating aggregate");
      }
    } else {
      if (newEndpoint.isSet(funcPath.getProperty())) {
        Object newValue = newEndpoint.get(funcPath.getProperty());
        Number newScalarValue = (Number) DataConverter.INSTANCE.convert(scalarType, funcPath
            .getProperty().getType(), newValue);
        switch (funcPath.getFunc().getName()) {
        case AVG: // Note; computing a sum until all values accumulated
          switch (scalarType) {
          case Double:
            Double avg = existingFuncValue != null ? existingFuncValue.doubleValue()
                + newScalarValue.doubleValue() : newScalarValue.doubleValue();
            existingEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), avg);
            break;
          case Float:
            Float floatAvg = existingFuncValue != null ? existingFuncValue.floatValue()
                + newScalarValue.floatValue() : newScalarValue.floatValue();
            existingEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), floatAvg);
            break;
          default:
            throw new IllegalArgumentException("illsgal datatype (" + scalarType
                + ") conversion for function, " + funcPath.getFunc().getName());
          }
          break;
        case MAX:

          if (existingFuncValue == null
              || Double.valueOf(newScalarValue.doubleValue()).compareTo(
                  existingFuncValue.doubleValue()) > 0)
            existingEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(),
                newScalarValue);
          break;
        case MIN:
          if (existingFuncValue == null
              || Double.valueOf(newScalarValue.doubleValue()).compareTo(
                  existingFuncValue.doubleValue()) < 0)
            existingEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(),
                newScalarValue);
          break;
        case SUM:
          switch (scalarType) {
          case Double:
            Double doubleSum = existingFuncValue != null ? existingFuncValue.doubleValue()
                + newScalarValue.doubleValue() : newScalarValue.doubleValue();
            if (!doubleSum.isInfinite()) {
              existingEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), doubleSum);
            } else { // overflow
              log.warn("aggregate " + funcPath.getFunc().getName() + " overflow for target type "
                  + scalarType + " - truncating aggregate");
            }
            break;
          case Long:
            Long longSum = existingFuncValue != null ? existingFuncValue.longValue()
                + newScalarValue.longValue() : newScalarValue.longValue();
            if (longSum > 0) {
              existingEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), longSum);
            } else { // overflow
              log.warn("aggregate " + funcPath.getFunc().getName() + " overflow for target type "
                  + scalarType + " - truncating aggregate");
            }
            break;
          default:
            throw new IllegalArgumentException("illegal datatype (" + scalarType
                + ") conversion for function, " + funcPath.getFunc().getName());
          }
          break;
        default:
          throw new GraphServiceException("unimplemented aggregate function, "
              + funcPath.getFunc().getName());
        }
      } else if (!funcPath.getProperty().isNullable())
        log.warn("expected value for non-nullable property, " + funcPath.getProperty());
    }

  }

  private void initAggregate(FunctionPath funcPath, PlasmaDataGraph graph) {
    DataType scalarType = funcPath.getFunc().getName().getScalarDatatype(funcPath.getDataType());
    PlasmaDataObject newEndpoint = null;
    if (funcPath.getPath().size() == 0) {
      newEndpoint = (PlasmaDataObject) graph.getRootObject();
    } else {
      newEndpoint = (PlasmaDataObject) graph.getRootObject().getDataObject(
          funcPath.getPath().toString());
    }

    if (funcPath.getFunc().getName().ordinal() == FunctionName.COUNT.ordinal()) {
      Long newCount = new Long(1);
      newEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), newCount);
      // FIXME: original scalar value is irrelevant for aggregates
      // but need for comparison
      // newEndpoint.unset(funcPath.getProperty());
    } else {
      if (newEndpoint.isSet(funcPath.getProperty())) {
        Object newValue = newEndpoint.get(funcPath.getProperty());
        Number newScalarValue = (Number) DataConverter.INSTANCE.convert(scalarType, funcPath
            .getProperty().getType(), newValue);
        // newEndpoint.unset(funcPath.getProperty());
        switch (funcPath.getFunc().getName()) {
        case AVG:
          switch (scalarType) {
          case Double:
            Double doubleAvg = newScalarValue.doubleValue();
            newEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), doubleAvg);
            break;
          case Float:
            Float floatAvg = newScalarValue.floatValue();
            newEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), floatAvg);
            break;
          default:
            throw new IllegalArgumentException("illsgal datatype (" + scalarType
                + ") conversion for function, " + funcPath.getFunc().getName());
          }
          break;
        case MAX:
          newEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), newScalarValue);
          break;
        case MIN:
          newEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), newScalarValue);
          break;
        case SUM:
          switch (scalarType) {
          case Double:
            Double sum = newScalarValue.doubleValue();
            newEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), sum);
            break;
          case Float:
            Float floatSum = newScalarValue.floatValue();
            newEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), floatSum);
            break;
          case Long:
            Long longSum = newScalarValue.longValue();
            newEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), longSum);
            break;
          default:
            throw new IllegalArgumentException("illsgal datatype (" + scalarType
                + ") conversion for function, " + funcPath.getFunc().getName());
          }
          break;
        case COUNT:
          break; // handled above
        default:
          throw new GraphServiceException("unimplemented aggregate function, "
              + funcPath.getFunc().getName());
        }
      } else if (!funcPath.getProperty().isNullable())
        log.warn("expected value for non-nullable property, " + funcPath.getProperty());
    }
  }

  private void computeAverages(PlasmaDataGraph graph) {
    CoreNode existingNode = (CoreNode) graph.getRootObject();
    int existingCount = (Integer) existingNode.getValueObject().get(PROPERTY_NAME_GROUP_HITS);
    for (FunctionPath funcPath : funcPaths) {
      if (!funcPath.getFunc().getName().isAggreate())
        throw new GraphServiceException("expected aggregate function not, "
            + funcPath.getFunc().getName());
      DataType scalarType = funcPath.getFunc().getName().getScalarDatatype(funcPath.getDataType());
      switch (funcPath.getFunc().getName()) {
      case AVG:
        PlasmaDataObject existingEndpoint = null;
        if (funcPath.getPath().size() == 0) {
          existingEndpoint = (PlasmaDataObject) graph.getRootObject();
        } else {
          existingEndpoint = (PlasmaDataObject) graph.getRootObject().getDataObject(
              funcPath.getPath().toString());
        }

        Number existingFuncValue = (Number) existingEndpoint.get(funcPath.getFunc().getName(),
            funcPath.getProperty());
        switch (scalarType) {
        case Double:
          Double doubleAvg = existingFuncValue.doubleValue() / existingCount;
          existingEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), doubleAvg);
          break;
        case Float:
          Float floatAvg = existingFuncValue.floatValue() / existingCount;
          existingEndpoint.set(funcPath.getFunc().getName(), funcPath.getProperty(), floatAvg);
          break;
        default:
          throw new IllegalArgumentException("illsgal datatype (" + scalarType
              + ") conversion for function, " + funcPath.getFunc().getName());
        }
      default:
        break;
      }
    }
  }

  @Override
  public int size() {
    return this.graphs.size();
  }

  @Override
  public PlasmaDataGraph[] getResults() {
    for (PlasmaDataGraph graph : graphs.values())
      this.computeAverages(graph);

    // wipe out the scalar value wherever we created an aggregate as the scalar
    // is no longer relevant. This is true with the exception of count()
    // aggregate on a specific column where the column/path is in the group by
    for (FunctionPath funcPath : funcPaths) {      
      if (funcPath.getFunc().getName().isAggreate()) {
        switch (funcPath.getFunc().getName()) {
        case COUNT:
          if (this.groupingComparator.contains(funcPath.getProperty(), funcPath.getPath()))
              continue; // next
          default:
            break;
        }
        
        for (PlasmaDataGraph graph : graphs.values()) {
          PlasmaDataObject endpoint = null;
          if (funcPath.getPath().size() == 0) {
            endpoint = (PlasmaDataObject) graph.getRootObject();
          } else {
            endpoint = (PlasmaDataObject) graph.getRootObject().getDataObject(
                funcPath.getPath().toString());
          }
          if (endpoint.isSet(funcPath.getProperty()))
            endpoint.unset(funcPath.getProperty());
        }
      }
    }

    List<PlasmaDataGraph> recognised = new ArrayList<PlasmaDataGraph>(graphs.size());
    if (this.havingSyntaxTree != null) {
      if (this.havingContext == null)
        this.havingContext = new GraphRecognizerContext();
      for (PlasmaDataGraph graph : graphs.values()) {
        this.havingContext.setGraph(graph);
        if (this.havingSyntaxTree.evaluate(this.havingContext)) {
          recognised.add(graph);
        } else {
          if (log.isDebugEnabled())
            log.debug("having recognizer excluded: " + graph);
          if (log.isDebugEnabled())
            log.debug(serializeGraph(graph));
        }
      }
    } else {
      recognised.addAll(graphs.values());
    }

    PlasmaDataGraph[] array = new PlasmaDataGraph[recognised.size()];
    recognised.toArray(array);
    if (this.orderingComparator != null)
      Arrays.sort(array, this.orderingComparator);
    return array;
  }

  @Override
  public PlasmaDataGraph getCurrentResult() {
    return this.current;
  }

}
