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

import java.util.HashMap;
import java.util.Map;

import org.plasma.query.model.GroupOperator;
import org.plasma.query.model.LogicalOperator;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.WildcardOperator;

/**
 * Encapsulates a single operator and associated precedence evaluation and other
 * logic.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public class Operator implements Comparable<Operator> {

	private org.plasma.query.Operator oper;
	private Object operValue;
	private Map<Object, Integer> precedenceMap = new HashMap<Object, Integer>();

	@SuppressWarnings("unused")
	private Operator() {
	}

	private Operator(Map<Object, Integer> precedenceMap) {
		this.precedenceMap = precedenceMap;
	}

	/**
	 * Constructs the encapsulated <a href=
	 * "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html>relational"
	 * ></a> operator along with its precedence map.
	 * 
	 * @param oper
	 *            the <a href=
	 *            "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html>relational"
	 *            ></a> operator
	 * @param precedenceMap
	 *            the precedence map
	 */
	public Operator(RelationalOperator oper, Map<Object, Integer> precedenceMap) {
		this(precedenceMap);
		this.oper = oper;
		this.operValue = oper.getValue();
	}

	/**
	 * Constructs the encapsulated <a href=
	 * "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html>logical"
	 * ></a> operator along with its precedence map.
	 * 
	 * @param oper
	 *            the <a href=
	 *            "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html>logical"
	 *            ></a> operator
	 * @param precedenceMap
	 *            the precedence map
	 */
	public Operator(LogicalOperator oper, Map<Object, Integer> precedenceMap) {
		this(precedenceMap);
		this.oper = oper;
		this.operValue = oper.getValue();
	}

	/**
	 * Constructs the encapsulated <a href=
	 * "http://docs.plasma-sdo.org/api/org/plasma/query/model/WildcardOperator.html>wildcard"
	 * ></a> operator along with its precedence map.
	 * 
	 * @param oper
	 *            the <a href=
	 *            "http://docs.plasma-sdo.org/api/org/plasma/query/model/WildcardOperator.html>wildcard"
	 *            ></a> operator
	 * @param precedenceMap
	 *            the precedence map
	 */
	public Operator(WildcardOperator oper, Map<Object, Integer> precedenceMap) {
		this(precedenceMap);
		this.oper = oper;
		this.operValue = oper.getValue();
	}

	/**
	 * Constructs the encapsulated <a href=
	 * "http://docs.plasma-sdo.org/api/org/plasma/query/model/GroupOperator.html>group"
	 * ></a> operator along with its precedence map.
	 * 
	 * @param oper
	 *            the <a href=
	 *            "http://docs.plasma-sdo.org/api/org/plasma/query/model/GroupOperator.html>group"
	 *            ></a> operator
	 * @param precedenceMap
	 *            the precedence map
	 */
	public Operator(GroupOperator oper, Map<Object, Integer> precedenceMap) {
		this(precedenceMap);
		this.oper = oper;
		this.operValue = oper.getValue();
	}

	/**
	 * Compares two operators using the precedence mapping.
	 * 
	 * @return the comparison result
	 */
	public int compareTo(Operator other) {
		Integer thisPrecedence = precedenceMap.get(this.operValue);
		if (thisPrecedence == null)
			throw new IllegalStateException(
					"no precedence found for operator, " + this.operValue);
		Integer otherPrecedence = precedenceMap.get(other.operValue);
		if (otherPrecedence == null)
			throw new IllegalStateException(
					"no precedence found for operator, " + other.operValue);

		return thisPrecedence.compareTo(otherPrecedence);
	}

	/**
	 * Returns the operator base interface
	 * 
	 * @return the operator base interface
	 */
	public org.plasma.query.Operator getOperator() {
		return oper;
	}

	public String toString() {
		return this.oper.getClass().getSimpleName() + " " + this.operValue;
	}
}
