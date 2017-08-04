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
package org.cloudgraph.hbase.scan;

import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.query.expr.EvaluationContext;

/**
 * Context which supports the "recognition" of one or more {@link PartialRowKey
 * partial}, {@link FuzzyRowKey fuzzy} and other and other scan constructs
 * within the context of a binary {@link Expr expression} syntax tree.
 * <p>
 * Composite row key scans represent only
 * {@link org.cloudgraph.hbase.expr.LogicalBinaryExpr logical binary} 'AND'
 * expressions across the key fields. So for
 * {@link org.cloudgraph.hbase.expr.RelationalBinaryExpr relational binary}
 * expressions linked within a query syntax tree by one or more logical binary
 * 'AND', expressions, a single {@link PartialRowKey partial} or
 * {@link FuzzyRowKey fuzzy} row key scan may be used. But for
 * {@link org.cloudgraph.hbase.expr.RelationalBinaryExpr relational binary}
 * expressions linked by {@link org.cloudgraph.hbase.expr.LogicalBinaryExpr
 * logical binary} 'OR' expressions multiple scans must be used. Clients of this
 * context class may execute the resulting scans in series or in parallel
 * depending on various performance and other considerations.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see org.cloudgraph.hbase.expr.Expr
 * @see org.cloudgraph.hbase.expr.BinaryExpr
 */
public class ScanRecognizerContext implements EvaluationContext {

  private DataGraphConfig graph;

  /**
   * Constructs an empty context.
   */
  public ScanRecognizerContext(DataGraphConfig graph) {
    this.graph = graph;
  }

  public DataGraphConfig getGraph() {
    return graph;
  }

}
