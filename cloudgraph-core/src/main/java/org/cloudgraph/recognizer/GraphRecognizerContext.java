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
package org.cloudgraph.recognizer;

import org.cloudgraph.query.expr.EvaluationContext;
import org.plasma.sdo.PlasmaDataGraph;

/**
 * Context which supports the evaluation or "recognition" of a given data graph
 * by a binary {@link Expr expression} tree, within the context of the
 * {@link Expr expression} syntax.
 * <p>
 * A graph recognizer is required when query expressions are present which
 * reference properties not found in the row key model for a target graph.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * 
 * @see org.cloudgraph.query.expr.Expr
 * @see org.cloudgraph.query.expr.BinaryExpr
 */
public class GraphRecognizerContext implements EvaluationContext {

	private PlasmaDataGraph graph;

	/**
	 * Constructs an empty context.
	 */
	public GraphRecognizerContext() {
	}

	public PlasmaDataGraph getGraph() {
		return graph;
	}

	public void setGraph(PlasmaDataGraph graph) {
		this.graph = graph;
	}

}
