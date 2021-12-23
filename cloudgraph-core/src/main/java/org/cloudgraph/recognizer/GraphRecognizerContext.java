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
package org.cloudgraph.recognizer;

import org.cloudgraph.query.expr.EvaluationContext;
import org.plasma.sdo.PlasmaDataGraph;

/**
 * Context which supports the evaluation or "recognition" of a given data graph
 * by a binary {@link Expr expression} tree, within the context of the
 * {@link Expr expression} syntax.
 * <p>
 * A graph recognizer is required when query expressions are present which
 * reference properties not found in the row key model for a target graph.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * 
 * @see org.cloudgraph.query.expr.Expr
 * @see org.cloudgraph.query.expr.BinaryExpr
 */
public class GraphRecognizerContext implements EvaluationContext {

  private PlasmaDataGraph graph;

  /**
   * Constructs an empty context.
   */
  public GraphRecognizerContext() {
  }

  public PlasmaDataGraph getGraph() {
    return graph;
  }

  public void setGraph(PlasmaDataGraph graph) {
    this.graph = graph;
  }

}
