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

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.key.CompositeRowKeyExpressionFactory;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;
import org.xml.sax.SAXException;

/**
 * Processes a <a href="http://docs.plasma-sdo.org/api/org/plasma/query/model/Where.html" target="#">where</a> predicate <a href="http://plasma-sdo.org/alldocs/plasma/api/org/plasma/query/model/Expression.html" target="#">expression</a> tree 
 * into a set of HBase <a href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/RowFilter.html" target="#">row filters</a> arranged 
 * within a hierarchy of HBase 
 * <a href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html" target="#">filter lists</a>. The
 * resulting <a href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html" target="#">filter list</a> resembles
 * the given expression tree with AND/OR <a href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ALL" target="#">MUST_PASS_ALL</a>/<a href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ONE" target="#">MUST_PASS_ONE</a> semantics 
 * representing the input.
 * A <a href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html" target="#">filter list</a> stack is
 * maintained which mirrors the query <a href="http://docs.plasma-sdo.org/api/org/plasma/query/model/Expression.html" target="#">expression</a> being
 * processed. 
 *  
 * @see CompositeRowKeyExpressionFactory 
 * @see org.cloudgraph.store.key.GraphRowKeyFactory
 * @see org.plasma.query.visitor.QueryVisitor
 * @see org.plasma.query.model.Query
 * @author Scott Cinnamond
 * @since 0.5
 */
public class PredicateRowFilterAssembler extends RowPredicateVisitor
    implements PredicateFilterAssembler
{
    private static Log log = LogFactory.getLog(PredicateRowFilterAssembler.class);

	/**
	 * Constructor sets up a {@link CompositeRowKeyExpressionFactory} for
	 * the given root type. 
	 * @param rootType the root type
     * @see CompositeRowKeyExpressionFactory 
	 */
	public PredicateRowFilterAssembler( 
			PlasmaType rootType) 
	{
		super(rootType);    	
        this.rowKeyFac = new CompositeRowKeyExpressionFactory(rootType);        
	}	
	
	/**
	 * Takes a {@link org.plasma.query.model.Query query} where
	 * clause containing any number of predicates and traverses
	 * these as a {org.plasma.query.visitor.QueryVisitor visitor} only
	 * processing various traversal events as needed against the 
	 * root type. 
	 * @param where the where clause
	 * @param contextType the context type
	 * @see org.plasma.query.visitor.QueryVisitor
	 * @see org.plasma.query.model.Query
	 */
	@Override
	public void assemble(Where where, PlasmaType contextType) {
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

    protected void log(Where root)
    {
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
