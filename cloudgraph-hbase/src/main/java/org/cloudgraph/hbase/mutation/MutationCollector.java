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
package org.cloudgraph.hbase.mutation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Row;
import org.cloudgraph.hbase.io.TableWriter;

import commonj.sdo.DataGraph;

/**
 * Traverses the change summary for one or more data graphs and collects changes in the form
 * of HBase row mutations.
 * <p>
 * For each graph: 
 * - performs any change summary ordering
 * - collects table writers based on graph metadata and configuration information
 * - assembles table writers for the graph into a single graph writer composed of table writers, which are
 * composed of row writers
 * - passes each changed object (created, modified, deleted) along with the graph writer to logic
 * within this class.    
 * - marshals out the state for each changed row after all changes complete in to the state column
 * - for each row, detects if the root object is deleted, and then adds a toumbsone column  
 * </p>
 *  
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public interface MutationCollector {

	public Map<TableWriter, List<Row>> collectChanges(DataGraph dataGraph) throws IOException, IllegalAccessException;

	public Map<TableWriter, List<Row>> collectChanges(DataGraph[] dataGraphs) throws IOException, IllegalAccessException;

	public void close();

}