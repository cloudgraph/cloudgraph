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
package org.cloudgraph.hbase.graph;

import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.cloudgraph.query.expr.EvaluationContext;

/**
 * Context which supports the evaluation and "recognition" of a given data graph
 * entity sequence value by a binary expression tree, within the context of the
 * expression syntax and a given HBase <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/KeyValue.html"
 * >KeyValue</a> map.
 * <p>
 * A sequence uniquely identifies an data graph entity within a local data graph
 * and is used to construct qualifier filters to return specific sequence-based
 * column qualifiers and associated column values.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * 
 * @see org.cloudgraph.hbase.graph.HBaseGraphAssembler
 * @see org.cloudgraph.hbase.graph.GraphSliceSupport
 */
public class LocalEdgeRecognizerContext implements EvaluationContext {

  private Map<String, KeyValue> keyMap;
  private Long sequence;

  /**
   * Constructs an empty context.
   */
  public LocalEdgeRecognizerContext() {
  }

  /**
   * Returns the HBase <a href=
   * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/KeyValue.html"
   * >KeyValue</a> specific for the current sequence.
   * 
   * @return the HBase <a href=
   *         "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/KeyValue.html"
   *         >KeyValue</a> specific for the current sequence.
   */
  public Map<String, KeyValue> getKeyMap() {
    return keyMap;
  }

  /**
   * Sets the HBase <a href=
   * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/KeyValue.html"
   * >KeyValue</a> specific for the current sequence.
   */
  public void setKeyMap(Map<String, KeyValue> keyMap) {
    this.keyMap = keyMap;
  }

  /**
   * Returns the current sequence.
   * 
   * @return the current sequence.
   */
  public Long getSequence() {
    return sequence;
  }

  /**
   * Sets the current sequence.
   */
  public void setSequence(Long sequence) {
    this.sequence = sequence;
  }
}
