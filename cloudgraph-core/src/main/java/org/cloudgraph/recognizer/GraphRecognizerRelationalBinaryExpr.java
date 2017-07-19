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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.query.expr.DefaultRelationalBinaryExpr;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.plasma.query.model.Literal;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * An {@link RelationalBinaryExpr} implementation which uses a specific 
 * evaluation {@link GraphRecognizerContext context} to evaluate
 * the value(s) of a data graph property along the query property
 * traversal path within the context of a binary expression (syntax) tree.       
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see GraphRecognizerContext
 * @see GraphRecognizerSupport
 */
public class GraphRecognizerRelationalBinaryExpr extends DefaultRelationalBinaryExpr 
    implements RelationalBinaryExpr {
    private static Log log = LogFactory.getLog(GraphRecognizerRelationalBinaryExpr.class);
 	protected PlasmaProperty endpointProperty;
 	protected GraphRecognizerSupport recognizer = new GraphRecognizerSupport();
	
    /**
     * Constructs an expression based on the given terms. 
     * @param property the "left" property term
     * @param literal the "right" literal term
     * @param operator the relational operator
     * @see EdgeRecognizerContext
     */
    public GraphRecognizerRelationalBinaryExpr(Property property,
			Literal literal, 
			RelationalOperator operator) {
		super(property, literal, operator);
	}
	
	/**
	 * Returns a "truth" value for the expression using a specific 
     * evaluation {@link GraphRecognizerContext context} by evaluate
     * the value of a property associated with the evaluation {@link GraphRecognizerContext context} 
     * within the binary expression tree.       
	 * @param context
	 * @return a "truth" value for the expression using a specific 
     * evaluation {@link GraphRecognizerContext context} by evaluate
     * the value of a property associated with the evaluation {@link GraphRecognizerContext context} 
     * within the binary expression tree.
     * @see GraphRecognizerContext
	 */
	@Override
	public boolean evaluate(EvaluationContext context) {
		GraphRecognizerContext ctx = (GraphRecognizerContext)context;		
				
		PlasmaDataGraph graph = ctx.getGraph();

		if (this.endpointProperty == null)
			this.endpointProperty = this.recognizer.getEndpoint(
				this.property,
				(PlasmaType)graph.getRootObject().getType());
        
		List<Object> values = new ArrayList<Object>();	
		this.recognizer.collect(graph.getRootObject(), this.property, 
				this.property.getPath(), 0, values);
		for (Object value : values) {
			if (this.recognizer.evaluate(this.endpointProperty, 
				value,
				this.operator.getValue(),
				this.literal.getValue())) {
				if (log.isDebugEnabled())
					log.debug(this.toString()+ " evaluate true: " + String.valueOf(value));
				return true;
			}
			else {
				if (log.isDebugEnabled())
					log.debug(this.toString()+ " evaluate false: " + String.valueOf(value));
			}
		}
		
		return false;
	}
	
}
