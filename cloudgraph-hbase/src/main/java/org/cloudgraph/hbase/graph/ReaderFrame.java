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

import org.cloudgraph.hbase.io.RowReader;

/**
 * Associates a row reader with a given graph 
 * (traversal) level. For use in detecting a target 
 * row reader based on its level. 
 * 
 * @see org.cloudgraph.hbase.io.RowReader
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class ReaderFrame {
	private RowReader rowReader;
	private int level;
	public ReaderFrame(RowReader rowReader, int level) {
		super();
		this.rowReader = rowReader;
		this.level = level;
	}
	public RowReader getRowReader() {
		return rowReader;
	}
	public int getLevel() {
		return level;
	}		
}
