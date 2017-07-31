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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.cloudgraph.query.expr.ExpresionVisitorSupport;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.model.NullLiteral;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;
import org.xml.sax.SAXException;

/**
 * Supports assembly of complex HBase filter hierarchies representing query
 * predicate expression trees using a filter stack.
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
public abstract class FilterHierarchyAssembler extends ExpresionVisitorSupport
		implements
			HBaseFilterAssembler {
	private static Log log = LogFactory.getLog(FilterHierarchyAssembler.class);

	protected List<Object> params = new ArrayList<Object>();
	protected FilterList rootFilter;
	protected Stack<FilterList> filterStack = new Stack<FilterList>();
	protected PlasmaType rootType;

	@SuppressWarnings("unused")
	private FilterHierarchyAssembler() {
	}
	protected FilterHierarchyAssembler(PlasmaType rootType) {
		this.rootType = rootType;
	}

	public void clear() {
		if (this.params != null)
			params.clear();
		this.filterStack.clear();
	}

	/**
	 * Returns the assembled filter, filter list or filter hierarchy root.
	 * 
	 * @return the assembled filter, filter list or or filter hierarchy root.
	 */
	public Filter getFilter() {
		return this.rootFilter;
	}

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

	protected void pushFilter() {
		pushFilter(FilterList.Operator.MUST_PASS_ALL);
	}

	protected void pushFilter(FilterList.Operator oper) {
		FilterList filter = new FilterList(oper);
		FilterList top = null;
		if (this.filterStack.size() > 0) {
			top = this.filterStack.peek();
			top.addFilter(filter);
		} else
			this.rootFilter = filter;
		this.filterStack.push(filter);
	}

	protected void popFilter() {
		this.filterStack.pop();
	}

	// String.split() can cause empty tokens under some circumstances
	protected String[] filterTokens(String[] tokens) {
		int count = 0;
		for (int i = 0; i < tokens.length; i++)
			if (tokens[i].length() > 0)
				count++;
		String[] result = new String[count];
		int j = 0;
		for (int i = 0; i < tokens.length; i++)
			if (tokens[i].length() > 0) {
				result[j] = tokens[i];
				j++;
			}
		return result;
	}

	protected void log(Where root) {
		String xml = "";
		PlasmaQueryDataBinding binding;
		try {
			binding = new PlasmaQueryDataBinding(
					new DefaultValidationEventHandler());
			xml = binding.marshal(root);
		} catch (JAXBException e) {
			log.debug(e);
		} catch (SAXException e) {
			log.debug(e);
		}
		log.debug("query: " + xml);
	}

}
