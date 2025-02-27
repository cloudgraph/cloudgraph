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

import org.plasma.query.model.Literal;
import org.plasma.query.model.Property;
import org.plasma.query.model.PredicateOperator;

/**
 * Represents an expression composed of two parts or terms joined by a <a href=
 * "http://docs.plasma-sdo.org/api/org/plasma/query/model/WildcardOperator.html"
 * >wildcard</a> operator.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public interface PredicateBinaryExpr extends BinaryExpr {
  /**
   * Returns the wildcard operator.
   * 
   * @return the wildcard operator.
   */
  public PredicateOperator getOperator();

  /**
   * Returns the property.
   * 
   * @return the property.
   */
  public Property getProperty();

  /**
   * Returns the string representation of the path qualified property.
   * 
   * @return the string representation of the path qualified property
   */
  public String getPropertyPath();

  /**
   * Returns the query literal
   * 
   * @return the query literal
   */
  public Literal getLiteral();
}
