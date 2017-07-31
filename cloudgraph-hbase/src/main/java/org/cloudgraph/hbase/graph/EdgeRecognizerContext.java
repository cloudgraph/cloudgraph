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

import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.cloudgraph.query.expr.EvaluationContext;

/**
 * Context which supports the evaluation and "recognition" of a given data graph
 * entity sequence value by a binary expression tree, within the context of the
 * expression syntax and a given HBase <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/KeyValue.html"
 * >KeyValue</a> map.
 * <p>
 * A sequence uniquely identifies an data graph entity within a local or
 * distributed data graph and is mapped internally to provide global uniqueness.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * 
 * @see org.cloudgraph.hbase.graph.HBaseGraphAssembler
 * @see org.cloudgraph.hbase.graph.GraphSliceSupport
 */
public class EdgeRecognizerContext implements EvaluationContext {

	private Map<String, KeyValue> keyMap;
	private Long sequence;

	/**
	 * Constructs an empty context.
	 */
	public EdgeRecognizerContext() {
	}

	/**
	 * Returns the HBase <a href=
	 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/KeyValue.html"
	 * >KeyValue</a> specific for the current sequence.
	 * 
	 * @return the HBase <a href=
	 *         "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/KeyValue.html"
	 *         >KeyValue</a> specific for the current sequence.
	 */
	public Map<String, KeyValue> getKeyMap() {
		return keyMap;
	}

	/**
	 * Sets the HBase <a href=
	 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/KeyValue.html"
	 * >KeyValue</a> specific for the current sequence.
	 */
	public void setKeyMap(Map<String, KeyValue> keyMap) {
		this.keyMap = keyMap;
	}

	/**
	 * Returns the current sequence.
	 * 
	 * @return the current sequence.
	 */
	public Long getSequence() {
		return sequence;
	}

	/**
	 * Sets the current sequence.
	 */
	public void setSequence(Long sequence) {
		this.sequence = sequence;
	}
}
