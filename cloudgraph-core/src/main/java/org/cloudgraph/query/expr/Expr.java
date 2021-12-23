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
package org.cloudgraph.query.expr;

/**
 * Base interface representing a query expression.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * 
 * @see EvaluationContext
 */
public interface Expr extends Term {

  /**
   * Returns a "truth" value for the expression based on the given context.
   * 
   * @param context
   * @return a "truth" value for the expression based on the given context.
   */
  public boolean evaluate(EvaluationContext context);

  /**
   * Begins the traversal of the expression as a root.
   * 
   * @param visitor
   *          the traversal visitor
   */
  public void accept(ExprVisitor visitor);
}
