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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.graph.EdgeRecognizerContext;
import org.cloudgraph.query.expr.DefaultRelationalBinaryExpr;
import org.cloudgraph.query.expr.EvaluationContext;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.plasma.query.model.Literal;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * An {@link RelationalBinaryExpr} implementation which uses a specific
 * evaluation {@link ScanRecognizerContext context} to evaluate the value(s) of
 * a data graph property along the query property traversal path within the
 * context of a binary expression (syntax) tree.
 * 
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see ScanRecognizerContext
 */
public class ScanRecognizerRelationalBinaryExpr
		extends
			DefaultRelationalBinaryExpr implements RelationalBinaryExpr {
	private static Log log = LogFactory
			.getLog(ScanRecognizerRelationalBinaryExpr.class);
	protected ScanLiteral scanLiteral;

	/**
	 * Constructs an expression based on the given terms.
	 * 
	 * @param property
	 *            the "left" property term
	 * @param literal
	 *            the "right" literal term
	 * @param operator
	 *            the relational operator
	 * @see EdgeRecognizerContext
	 */
	public ScanRecognizerRelationalBinaryExpr(Property property,
			Literal literal, RelationalOperator operator) {
		super(property, literal, operator);
	}

	/**
	 * Returns a "truth" value for the expression using a specific evaluation
	 * {@link ScanRecognizerContext context} by ... within the binary expression
	 * tree.
	 * 
	 * @param context
	 * @return a "truth" value for the expression
	 * @see ScanRecognizerContext
	 */
	@Override
	public boolean evaluate(EvaluationContext context) {
		ScanRecognizerContext ctx = (ScanRecognizerContext) context;
		ScanLiteral literal = createLiteral(ctx.getGraph());

		return false;
	}

	private ScanLiteral createLiteral(DataGraphConfig graph) {
		// Match the current property to a user defined
		// row key token, if found we can process
		UserDefinedRowKeyFieldConfig fieldConfig = graph
				.getUserDefinedRowKeyField(this.propertyPath);
		if (fieldConfig != null) {
			PlasmaProperty property = (PlasmaProperty) fieldConfig
					.getEndpointProperty();
			ScanLiteralFactory factory = new ScanLiteralFactory();

			ScanLiteral scanLiteral = factory.createLiteral(
					this.literal.getValue(), property,
					(PlasmaType) graph.getRootType(), this.operator,
					fieldConfig);
			return scanLiteral;
		} else
			log.warn("no user defined row-key field for query path '"
					+ this.propertyPath
					+ "' - deferring to graph recogniser post processor");

		return null;
	}

}
