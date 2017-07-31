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
package org.cloudgraph.hbase.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.key.CompositeColumnKeyFactory;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * Creates an HBase value and qualifier filter hierarchy to return HBase scan
 * results representing part of a graph "slice".
 * 
 * Uses <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/QualifierFilter.html"
 * >QualifierFilter</a> /<a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/ValueFilter.html"
 * >ValueFilter</a> pairs recreating composite column qualifier prefixes using
 * {@link CompositeColumnKeyFactory}. Processes visitor events for query model
 * elements specific to assembly of HBase column filters, such as properties,
 * wildcards, literals, logical operators, relational operators, within the
 * context of HBase filter hierarchy assembly. Maintains various context
 * information useful to subclasses.
 * <p>
 * HBase filters may be collected into lists using <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html"
 * target="#">FilterList</a> each with a <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ALL"
 * target="#">MUST_PASS_ALL</a> or <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ONE"
 * target="#">MUST_PASS_ONE</a> (logical) operator. Lists may then be assembled
 * into hierarchies used to represent complex expression trees filtering either
 * rows or columns in HBase.
 * </p>
 * 
 * @see org.cloudgraph.common.key.CompositeColumnKeyFactory
 * @author Scott Cinnamond
 * @since 0.5
 */
public class ColumnPredicateFilterAssembler extends ColumnPredicateVisitor
		implements
			PredicateFilterAssembler {
	private static Log log = LogFactory
			.getLog(ColumnPredicateFilterAssembler.class);

	/**
	 * Constructor which takes a {@link org.plasma.query.model.Query query}
	 * where clause containing any number of predicates and traverses these as a
	 * {org.plasma.query.visitor.QueryVisitor visitor} only processing various
	 * traversal events as needed against the given root type.
	 * 
	 * @param where
	 *            where the where clause
	 * @param contextType
	 *            the current SDO type
	 * @param rootType
	 *            the graph root type
	 * @see org.plasma.query.visitor.QueryVisitor
	 * @see org.plasma.query.model.Query
	 */
	public ColumnPredicateFilterAssembler(PlasmaType rootType) {
		super(rootType);
		this.columnKeyFac = new CompositeColumnKeyFactory(rootType);
	}

	@Override
	public void assemble(Where where, PlasmaType contextType) {
		this.contextType = contextType;
		for (int i = 0; i < where.getParameters().size(); i++)
			params.add(where.getParameters().get(i).getValue());

		if (log.isDebugEnabled())
			this.log(where);

		if (log.isDebugEnabled())
			log.debug("begin traverse");

		where.accept(this); // traverse

		if (log.isDebugEnabled())
			log.debug("end traverse");
	}

	public void clear() {
		super.clear();
	}

}
