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

import java.util.HashMap;
import java.util.Map;

import org.plasma.query.model.GroupOperator;
import org.plasma.query.model.LogicalOperator;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.PredicateOperator;

/**
 * Encapsulates a single operator and associated precedence evaluation and other
 * logic.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public class Operator implements Comparable<Operator> {

  private org.plasma.query.Operator oper;
  private Object operValue;
  private Map<Object, Integer> precedenceMap = new HashMap<Object, Integer>();

  @SuppressWarnings("unused")
  private Operator() {
  }

  private Operator(Map<Object, Integer> precedenceMap) {
    this.precedenceMap = precedenceMap;
  }

  /**
   * Constructs the encapsulated <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html>relational"
   * ></a> operator along with its precedence map.
   * 
   * @param oper
   *          the <a href=
   *          "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html>relational"
   *          ></a> operator
   * @param precedenceMap
   *          the precedence map
   */
  public Operator(RelationalOperator oper, Map<Object, Integer> precedenceMap) {
    this(precedenceMap);
    this.oper = oper;
    this.operValue = oper.getValue();
  }

  /**
   * Constructs the encapsulated <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html>logical"
   * ></a> operator along with its precedence map.
   * 
   * @param oper
   *          the <a href=
   *          "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html>logical"
   *          ></a> operator
   * @param precedenceMap
   *          the precedence map
   */
  public Operator(LogicalOperator oper, Map<Object, Integer> precedenceMap) {
    this(precedenceMap);
    this.oper = oper;
    this.operValue = oper.getValue();
  }

  /**
   * Constructs the encapsulated <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/WildcardOperator.html>wildcard"
   * ></a> operator along with its precedence map.
   * 
   * @param oper
   *          the <a href=
   *          "http://docs.plasma-sdo.org/api/org/plasma/query/model/WildcardOperator.html>wildcard"
   *          ></a> operator
   * @param precedenceMap
   *          the precedence map
   */
  public Operator(PredicateOperator oper, Map<Object, Integer> precedenceMap) {
    this(precedenceMap);
    this.oper = oper;
    this.operValue = oper.getValue();
  }

  /**
   * Constructs the encapsulated <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/GroupOperator.html>group"
   * ></a> operator along with its precedence map.
   * 
   * @param oper
   *          the <a href=
   *          "http://docs.plasma-sdo.org/api/org/plasma/query/model/GroupOperator.html>group"
   *          ></a> operator
   * @param precedenceMap
   *          the precedence map
   */
  public Operator(GroupOperator oper, Map<Object, Integer> precedenceMap) {
    this(precedenceMap);
    this.oper = oper;
    this.operValue = oper.getValue();
  }

  /**
   * Compares two operators using the precedence mapping.
   * 
   * @return the comparison result
   */
  public int compareTo(Operator other) {
    Integer thisPrecedence = precedenceMap.get(this.operValue);
    if (thisPrecedence == null)
      throw new IllegalStateException("no precedence found for operator, " + this.operValue);
    Integer otherPrecedence = precedenceMap.get(other.operValue);
    if (otherPrecedence == null)
      throw new IllegalStateException("no precedence found for operator, " + other.operValue);

    return thisPrecedence.compareTo(otherPrecedence);
  }

  /**
   * Returns the operator base interface
   * 
   * @return the operator base interface
   */
  public org.plasma.query.Operator getOperator() {
    return oper;
  }

  public String toString() {
    return this.oper.getClass().getSimpleName() + " " + this.operValue;
  }
}
