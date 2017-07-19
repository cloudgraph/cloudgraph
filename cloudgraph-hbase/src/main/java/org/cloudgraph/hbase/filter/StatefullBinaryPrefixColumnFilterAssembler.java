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


import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.key.StatefullColumnKeyFactory;
import org.cloudgraph.store.key.EntityMetaField;
import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

/**
 * Creates an HBase column filter list using <a target="#" href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/QualifierFilter.html">QualifierFilter</a> 
 * and <a target="#" href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/BinaryPrefixComparator.html">BinaryPrefixComparator</a> and
 * recreating composite column qualifier prefixes for comparison using {@link StatefullColumnKeyFactory}. 
 * <p>
 * HBase filters may be collected into 
 * lists using <a href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html" target="#">FilterList</a>
 * each with a 
 * <a href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ALL" target="#">MUST_PASS_ALL</a> or <a href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ONE" target="#">MUST_PASS_ONE</a>
 *  (logical) operator. Lists may then be assembled into hierarchies 
 * used to represent complex expression trees filtering either rows
 * or columns in HBase.
 * </p> 
 * @see org.cloudgraph.store.key.GraphStatefullColumnKeyFactory
 * @see org.cloudgraph.hbase.key.StatefullColumnKeyFactory
 * @author Scott Cinnamond
 * @since 0.5
 */
public class StatefullBinaryPrefixColumnFilterAssembler extends FilterListAssembler
{
    private static Log log = LogFactory.getLog(StatefullBinaryPrefixColumnFilterAssembler.class);
	private GraphStatefullColumnKeyFactory columnKeyFac;
	private EdgeReader edgeReader;

	public StatefullBinaryPrefixColumnFilterAssembler( 
			PlasmaType rootType, EdgeReader edgeReader) 
	{
		super(rootType);
		this.edgeReader = edgeReader;
    	this.rootFilter = new FilterList(
    		FilterList.Operator.MUST_PASS_ONE);
    	 
        this.columnKeyFac = new StatefullColumnKeyFactory(rootType);  
	} 
	
	public void assemble(Set<Property> properies, Set<Long> sequences,
		PlasmaType contextType) 
	{
		byte[] colKey = null;
		QualifierFilter qualFilter = null;
    	PlasmaType subType = edgeReader.getSubType();
    	if (subType == null)
    		subType = edgeReader.getBaseType();
		
        for (Long seq : sequences) {        	
			// adds entity level meta data qualifier prefixes for ALL sequences
			// in the selection
        	for (EntityMetaField metaField : EntityMetaField.values()) {
			    colKey = this.columnKeyFac.createColumnKey(subType, seq, metaField);
                qualFilter = new QualifierFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator(colKey)); 
                this.rootFilter.addFilter(qualFilter);
            }
       	
        	for (Property p : properies) {
        		PlasmaProperty prop = (PlasmaProperty)p;
        		colKey = this.columnKeyFac.createColumnKey(subType, 
        		    seq, prop);
                qualFilter = new QualifierFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator(colKey)); 
                this.rootFilter.addFilter(qualFilter);
        	}
        }        
	}
}	

