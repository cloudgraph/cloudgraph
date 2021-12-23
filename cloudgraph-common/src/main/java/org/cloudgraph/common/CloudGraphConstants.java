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

package org.cloudgraph.common;

import org.plasma.sdo.core.CoreConstants;

/**
 * Common constants.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CloudGraphConstants {

  /**
   * An SDO instance property representing the time in milliseconds taken to
   * assemble a data graph.
   */
  public static final String GRAPH_ASSEMBLY_TIME = "GraphAssemblyTime";

  /**
   * An SDO instance property representing the total time in milliseconds taken
   * for round trips to the underlying data store(s) while assembling a data
   * graph.
   */
  public static final String GRAPH_FETCH_TIME = "GraphFetchTime";

  /**
   * An SDO instance property representing the number of graph nodes contained
   * by a data graph.
   */
  public static final String GRAPH_NODE_COUNT = "GraphNodeCount";

  /**
   * An SDO instance property representing the number of threads used to
   * assemble a data graph.
   */
  public static final String GRAPH_THREAD_COUNT = "GraphThreadCount";

  /**
   * An SDO instance property representing the name of the thread used to
   * assemble a particular graph node.
   */
  public static final String GRAPH_NODE_THREAD_NAME = "GraphNodeThreadName";

  /**
   * An SDO instance property representing the depth of a data graph.
   */
  public static final String GRAPH_DEPTH = "GraphDepth";

  /**
   * An SDO instance property representing the table names for all physical
   * tables used to assemble a graph.
   */
  public static final String GRAPH_TABLE_NAMES = "GraphTableNames";

  /**
   * An SDO instance property used to set / cache row keys on the root and other
   * nodes across a distributed graph.
   */
  public static final String ROW_KEY = CoreConstants.PROPERTY_NAME_ROWKEY;

  /**
   * An SDO instance property used to store the sequence associated with each
   * data object. The sequences are available on graph assembly, and needed for
   * fast construction of column qualifiers during update and delete operations.
   */
  public static final String SEQUENCE = CoreConstants.PROPERTY_NAME_SEQUENCE;

  /**
   * The configuration property name for the property which indicates the
   * non-cryptographic hash algorithm type.
   */
  public static final String PROPERTY_CONFIG_HASH_TYPE = "hash.type";

  @Deprecated
  public static final long ROOT_SEQUENCE = -1;

}
