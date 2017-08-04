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

import org.plasma.query.model.ArithmeticOperator;

/**
 * Represents an expression composed of two parts or terms joined by an <a href=
 * "http://docs.plasma-sdo.org/api/org/plasma/query/model/ArithmeticOperator.html"
 * >arithmetic</a> operator.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public interface ArithmeticBinaryExpr extends BinaryExpr {
  /**
   * Returns the arithmetic operator.
   * 
   * @return the arithmetic operator.
   */
  public ArithmeticOperator getOperator();
}
