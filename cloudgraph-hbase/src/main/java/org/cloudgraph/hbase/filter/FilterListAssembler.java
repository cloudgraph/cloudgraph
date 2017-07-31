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

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.plasma.query.model.NullLiteral;
import org.plasma.sdo.PlasmaType;

/**
 * Supports assembly of HBase filter lists.
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
 * @author Scott Cinnamond
 * @since 0.5
 */
public abstract class FilterListAssembler implements HBaseFilterAssembler {
	private static Log log = LogFactory.getLog(FilterListAssembler.class);

	protected List<Object> params;
	protected FilterList rootFilter;
	protected PlasmaType rootType;

	@SuppressWarnings("unused")
	private FilterListAssembler() {
	}
	protected FilterListAssembler(PlasmaType rootType) {
		this.rootType = rootType;

		this.rootFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	}

	/**
	 * Returns the assembled filter or filter list root.
	 * 
	 * @return the assembled filter or filter list root.
	 */
	public Filter getFilter() {
		return rootFilter;
	}

	public void clear() {
		if (params != null)
			params.clear();
		this.rootFilter.getFilters().clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cloudgraph.hbase.filter.HBaseFilterAssembler#getParams()
	 */
	public Object[] getParams() {
		Object[] result = new Object[params.size()];
		Iterator<Object> iter = params.iterator();
		for (int i = 0; iter.hasNext(); i++) {
			Object param = iter.next();
			if (!(param instanceof NullLiteral))
				result[i] = param;
			else
				result[i] = null;
		}
		return result;
	}

}
