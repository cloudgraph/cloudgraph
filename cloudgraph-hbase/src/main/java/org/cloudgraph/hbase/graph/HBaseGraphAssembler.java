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
package org.cloudgraph.hbase.graph;

import org.apache.hadoop.hbase.client.Result;
import org.plasma.sdo.access.DataGraphAssembler;

/**
 * Constructs a data graph starting with a given root SDO type based on a given
 * HBase client <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Result.html"
 * >result</a> row the entire graph being found in the result row or potentially
 * requiring additional scans depending on the assembler implementation.
 * <p>
 * Since every column key in HBase must be unique, and a data graph may contain
 * any number of nodes, a column key factory is used both to persist as well as
 * re-constitute a graph. A minimal amount of "state" information is therefore
 * stored with each graph which maps user readable sequence numbers (which are
 * used in column keys) to UUID values. The nodes of the resulting data graph
 * are re-created with the original UUID values.
 * </p>
 * 
 * @see org.cloudgraph.hbase.key.StatefullColumnKeyFactory
 * @see org.plasma.sdo.PlasmaDataGraph
 * @see org.apache.hadoop.hbase.client.Result
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface HBaseGraphAssembler extends DataGraphAssembler {
  /**
   * Re-constitutes a data graph from the given HBase client result (row).
   * 
   * @param resultRow
   *          the HBase client result (row).
   */
  public void assemble(Result resultRow);

}