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
package org.cloudgraph.aerospike.graph;

import java.util.Collection;

import org.cloudgraph.aerospike.key.CompositeRowKeyReader;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.recognizer.Endpoint;
import org.cloudgraph.store.key.KeyValue;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.sdo.PlasmaType;

/**
 * Context which supports the evaluation and "recognition" of a given external
 * reference or row-key by a binary expression tree.
 * <p>
 * A sequence uniquely identifies an data graph entity within a local or
 * distributed data graph and is mapped internally to provide global uniqueness.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 1.0.4
 * 
 * @see org.cloudgraph.aerospike.graph.HBaseGraphAssembler
 * @see org.cloudgraph.aerospike.graph.GraphSliceSupport
 * @see CompositeRowKeyReader
 */
public class ExternalEdgeRecognizerContext implements EvaluationContext {

  private CompositeRowKeyReader rowKeyReader;
  /**
   * Whether the row contains all fields represented in the predicate
   * expressions and all predicates were evaluated successfully.
   */
  private boolean rowEvaluatedCompletely;

  /**
   * Constructs an empty context.
   */
  public ExternalEdgeRecognizerContext(PlasmaType contextType, StoreMappingContext mappingContext) {
    this.rowKeyReader = new CompositeRowKeyReader(contextType, mappingContext);
  }

  public void read(byte[] rowKey) {
    this.rowKeyReader.read(rowKey);
    this.rowEvaluatedCompletely = true;
  }

  public Object getValue(Endpoint endpoint) {
    return this.rowKeyReader.getValue(endpoint);
  }

  public Collection<Endpoint> getEndpoints() {
    return rowKeyReader.getEndpoints();
  }

  public Collection<KeyValue> getValues() {
    return this.rowKeyReader.getValues();
  }

  public PlasmaType getContextType() {
    return this.rowKeyReader.getContextType();
  }

  public boolean isRowEvaluatedCompletely() {
    return rowEvaluatedCompletely;
  }

  void setRowEvaluatedCompletely(boolean rowEvaluatedCompletely) {
    this.rowEvaluatedCompletely = rowEvaluatedCompletely;
  }

}
