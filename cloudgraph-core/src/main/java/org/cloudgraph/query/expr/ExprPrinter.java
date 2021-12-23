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
 * Simple visitor client which creates a "printable" representation of a binary
 * expression tree for debugging purposes.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * 
 * @see Expr
 */
public class ExprPrinter implements ExprVisitor {
  StringBuilder buf = new StringBuilder();

  @Override
  public void visit(Expr target, Expr source, int level) {
    buf.append("\n");
    for (int i = 0; i < level; i++)
      buf.append("\t");
    buf.append(target.toString());

  }

  public String toString() {
    return buf.toString();
  }
}
