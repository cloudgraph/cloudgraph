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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.key.CompositeColumnKeyFactory;
import org.cloudgraph.store.key.EntityMetaField;
import org.cloudgraph.store.key.GraphColumnKeyFactory;
import org.cloudgraph.store.key.GraphMetaField;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.collector.Selection;
import org.plasma.query.model.Select;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.xml.sax.SAXException;

import commonj.sdo.Property;
import commonj.sdo.Type;

/**
 * Creates an HBase column filter set based on the given criteria
 * which leverages the HBase <a target="#" href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/MultipleColumnPrefixFilter.html">MultipleColumnPrefixFilter</a>
 * to return only columns which make up the selected data graph. The advantage of this strategy is that
 * a complete graph of any complexity may be returned in a single round trip. 
 *  
 * @see GraphColumnKeyFactory
 * @see InitialFetchColumnFilterAssembler
 * @author Scott Cinnamond
 * @since 0.5
 */
public class GraphFetchColumnFilterAssembler extends FilterListAssembler 
    implements HBaseFilterAssembler
{
    private static Log log = LogFactory.getLog(GraphFetchColumnFilterAssembler.class);

	private GraphColumnKeyFactory columnKeyFac;
	private Map<String, byte[]> prefixMap = new HashMap<String, byte[]>();
    private Selection propertySelection;
	
	public GraphFetchColumnFilterAssembler( 
			Selection selection,
			PlasmaType rootType) {
		
		super(rootType);
		this.propertySelection = selection;
        this.columnKeyFac = new CompositeColumnKeyFactory(rootType);
		
    	this.rootFilter = new FilterList(
    			FilterList.Operator.MUST_PASS_ONE);

    	// add default filters for graph state info needed for all queries
    	QualifierFilter filter = null;
		for (GraphMetaField field : GraphMetaField.values()) {
	        filter = new QualifierFilter(
	            	CompareFilter.CompareOp.EQUAL,
	            	new SubstringComparator(field.asString()));   
	        this.rootFilter.addFilter(filter);
		}
    	
    	collect();

    	byte[][] prefixes = new byte[this.prefixMap.size()][];
    	int i = 0;
    	for (byte[] prefix : this.prefixMap.values()) {
    		prefixes[i] = prefix; 
    		i++;
    	}    	
        MultipleColumnPrefixFilter multiPrefixfilter = 
        	new MultipleColumnPrefixFilter(prefixes);
        
        this.rootFilter.addFilter(multiPrefixfilter);  
          	
	}
	
	public void clear() {
		super.clear();
		this.prefixMap.clear();
	}
	
	/**
	 * Collects and maps column prefixes used to create HBase filter(s)
	 * for column selection. 
	 * @param select the select clause
	 */
	private void collect() {
		byte[] colKey = null;
		String colKeyStr;
		for (Type type : this.propertySelection.getInheritedTypes()) {
			PlasmaType plasmaType = (PlasmaType)type;
			// adds entity level meta data qualifier prefixes for ALL types
			// in the selection
			for (EntityMetaField metaField : EntityMetaField.values()) {
			    colKey = this.columnKeyFac.createColumnKey(plasmaType, metaField);
			    colKeyStr = Bytes.toString(colKey);
			    this.prefixMap.put(colKeyStr, colKey);
			}
			
			Set<Property> props = this.propertySelection.getInheritedProperties(type);
            for (Property prop : props) {
                colKey = this.columnKeyFac.createColumnKey(
                		plasmaType, (PlasmaProperty)prop);
                colKeyStr = Bytes.toString(colKey);
                this.prefixMap.put(colKeyStr, colKey);
            }
		}
	}
		
    protected void log(Select root)
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
    }}
