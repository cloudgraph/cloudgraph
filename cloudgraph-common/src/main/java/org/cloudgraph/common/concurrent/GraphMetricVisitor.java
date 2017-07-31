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
package org.cloudgraph.common.concurrent;

import java.util.HashSet;

import org.cloudgraph.common.CloudGraphConstants;
import org.plasma.sdo.PlasmaDataGraphVisitor;
import org.plasma.sdo.core.CoreNode;

import commonj.sdo.DataObject;

public class GraphMetricVisitor implements PlasmaDataGraphVisitor {

	private long count = 0;
	private long depth = 0;
	private HashSet<String> threadNames = new HashSet<String>();

	@Override
	public void visit(DataObject target, DataObject source,
			String sourcePropertyName, int level) {
		count++;
		if (level > depth)
			depth = level;

		CoreNode node = (CoreNode) target;
		String thread = (String) node.getValueObject().get(
				CloudGraphConstants.GRAPH_NODE_THREAD_NAME);
		if (thread != null)
			this.threadNames.add(thread);

	}

	public long getCount() {
		return count;
	}

	public long getDepth() {
		return depth;
	}

	public long getThreadCount() {
		return threadNames.size();
	}
}
