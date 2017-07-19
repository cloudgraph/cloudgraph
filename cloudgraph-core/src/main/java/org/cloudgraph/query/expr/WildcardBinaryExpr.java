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
package org.cloudgraph.query.expr;

import org.plasma.query.model.Literal;
import org.plasma.query.model.Property;
import org.plasma.query.model.WildcardOperator;

/**
 * Represents an expression composed of two parts or terms
 * joined by a <a href="http://docs.plasma-sdo.org/api/org/plasma/query/model/WildcardOperator.html">wildcard</a> operator.
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public interface WildcardBinaryExpr extends BinaryExpr {
	/**
	 * Returns the wildcard operator.
	 * @return the wildcard operator.
	 */
	public WildcardOperator getOperator();
	/**
	 * Returns the property.
	 * @return the property.
	 */
	public Property getProperty();

	/**
	 * Returns the string representation of the path qualified
	 * property.
	 * @return the string representation of the path qualified
	 * property
	 */
 	public String getPropertyPath();

 	/**
 	 * Returns the query literal
 	 * @return the query literal
 	 */
	public Literal getLiteral();
}
