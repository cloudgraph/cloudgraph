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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.cloudgraph.query.expr.DefaultRelationalBinaryExpr;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.plasma.query.model.Literal;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;

/**
 * An {@link RelationalBinaryExpr} implementation which uses a specific 
 * evaluation {@link EdgeRecognizerContext context} to locate
 * or recognize a given sequence based column qualifier within 
 * the context of the expression.       
 * @author Scott Cinnamond
 * @since 0.5.2
 * @see EdgeRecognizerContext
 */
public class EdgeRecognizerRelationalBinaryExpr extends DefaultRelationalBinaryExpr 
    implements RelationalBinaryExpr {
    private static Log log = LogFactory.getLog(EdgeRecognizerRelationalBinaryExpr.class);
    private String columnQualifierPrefix;
	
    /**
     * Constructs an expression based on the given terms
     * and column qualifier prefix. 
     * @param property the "left" property term
     * @param columnQualifierPrefix the qualifier prefix used
     * to evaluate the expression for a given context.
     * @param literal the "right" literal term
     * @param operator the relational operator
     * @see EdgeRecognizerContext
     */
    public EdgeRecognizerRelationalBinaryExpr(Property property,
			String columnQualifierPrefix,
			Literal literal, 
			RelationalOperator operator) {
		super(property, literal, operator);
		this.columnQualifierPrefix = columnQualifierPrefix;		
	}
	
	/**
	 * Returns a "truth" value for the expression using a specific 
     * evaluation {@link EdgeRecognizerContext context} to locate
     * or recognize a given sequence based column qualifier within 
     * the context of the expression.   
	 * @param context
	 * @return a "truth" value for the expression using a specific 
     * evaluation {@link EdgeRecognizerContext context} to locate
     * or recognize a given sequence based column qualifier within 
     * the context of the expression.
     * @see EdgeRecognizerContext
	 */
	@Override
	public boolean evaluate(EvaluationContext context) {
		EdgeRecognizerContext ctx = (EdgeRecognizerContext)context;		
		//FIXME: use Array copy
		String qualifier = this.columnQualifierPrefix 
				+ String.valueOf(ctx.getSequence());
		KeyValue value = ctx.getKeyMap().get(qualifier);
		
		
		boolean found = value != null;
		if (log.isDebugEnabled())
			log.debug("evaluate: " + found + " '" + qualifier
				+ "' in map " + ctx.getKeyMap().keySet());
		return found;
	}
	    
}
