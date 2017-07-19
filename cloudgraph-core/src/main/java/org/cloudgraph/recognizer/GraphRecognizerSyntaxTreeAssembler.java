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

import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.query.expr.DefaultBinaryExprTreeAssembler;
import org.cloudgraph.query.expr.ExprAssembler;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.cloudgraph.query.expr.WildcardBinaryExpr;
import org.plasma.query.model.Literal;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.Where;
import org.plasma.query.model.WildcardOperator;
import org.plasma.sdo.PlasmaType;

/**
 * A binary expression tree assembler which constructs an operator 
 * precedence map, then {@link org.cloudgraph.query.expr.ExpresionVisitorSupport visits} (traverses) 
 * the given predicate expression syntax tree depth-first 
 * using an adapted shunting-yard algorithm and assembles a 
 * resulting binary tree structure with expression nodes specific
 * for detecting i.e. recognizing a data graph. In typical usage 
 * scenarios, a single expression tree is assembled once, and 
 * then used to evaluate any number of data graphs.
 * <p>
 * The adapted shunting-yard algorithm in general uses a stack of 
 * operators and operands, and as new binary tree nodes are detected and 
 * created they are pushed onto the operand stack based on operator precedence.
 * The resulting binary expression tree reflects the syntax of the
 * underlying query expression including the precedence of its operators.
 * </p>
 *   
 * @author Scott Cinnamond
 * @since 0.5.3
 * 
 * @see ExprAssembler
 * @see DefaultBinaryExprTreeAssembler
 */
public class GraphRecognizerSyntaxTreeAssembler extends DefaultBinaryExprTreeAssembler 
{
	protected DataGraphConfig graphConfig;
	
	/**
	 * Constructs an assembler based on the given predicate
	 * data graph configuration and graph root type.
	 * @param predicate the predicate
	 * @param rootType the graph root type
	 */
	public GraphRecognizerSyntaxTreeAssembler(Where predicate,
			PlasmaType rootType) {
		super(predicate, rootType);
	}
	
	@Override
	public RelationalBinaryExpr createRelationalBinaryExpr(Property property,
			Literal literal, RelationalOperator operator) {
	    return new GraphRecognizerRelationalBinaryExpr(
	    		property, literal, operator);
	}
		
	@Override
	public WildcardBinaryExpr createWildcardBinaryExpr(Property property,
			Literal literal, WildcardOperator operator) {
	    return new GraphRecognizerWildcardBinaryExpr(
	    		property, literal, operator);
	}
}
