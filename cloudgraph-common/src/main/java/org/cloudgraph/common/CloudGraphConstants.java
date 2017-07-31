/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
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
