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
package org.cloudgraph.hbase.scan;

import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.query.expr.EvaluationContext;

/**
 * Context which supports the "recognition" of one or more {@link PartialRowKey
 * partial}, {@link FuzzyRowKey fuzzy} and other and other scan constructs
 * within the context of a binary {@link Expr expression} syntax tree.
 * <p>
 * Composite row key scans represent only
 * {@link org.cloudgraph.hbase.expr.LogicalBinaryExpr logical binary} 'AND'
 * expressions across the key fields. So for
 * {@link org.cloudgraph.hbase.expr.RelationalBinaryExpr relational binary}
 * expressions linked within a query syntax tree by one or more logical binary
 * 'AND', expressions, a single {@link PartialRowKey partial} or
 * {@link FuzzyRowKey fuzzy} row key scan may be used. But for
 * {@link org.cloudgraph.hbase.expr.RelationalBinaryExpr relational binary}
 * expressions linked by {@link org.cloudgraph.hbase.expr.LogicalBinaryExpr
 * logical binary} 'OR' expressions multiple scans must be used. Clients of this
 * context class may execute the resulting scans in series or in parallel
 * depending on various performance and other considerations.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see org.cloudgraph.hbase.expr.Expr
 * @see org.cloudgraph.hbase.expr.BinaryExpr
 */
public class ScanRecognizerContext implements EvaluationContext {

	private DataGraphConfig graph;

	/**
	 * Constructs an empty context.
	 */
	public ScanRecognizerContext(DataGraphConfig graph) {
		this.graph = graph;
	}

	public DataGraphConfig getGraph() {
		return graph;
	}

}
