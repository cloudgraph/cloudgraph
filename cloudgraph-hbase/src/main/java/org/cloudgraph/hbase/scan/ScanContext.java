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

import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.QueryException;
import org.plasma.query.model.GroupOperator;
import org.plasma.query.model.LogicalOperator;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.Where;
import org.plasma.query.model.WildcardOperator;
import org.plasma.query.visitor.DefaultQueryVisitor;
import org.plasma.sdo.PlasmaType;

/**
 * Conducts an initial traversal while capturing and 
 * analyzing the characteristics of a query in order to 
 * leverage the important HBase partial row-key scan
 * capability for every possible predicate expression. 
 * <p>
 * Based on the various access methods, if a client determines that
 * an HBase partial row-key scan is possible based, a {@link PartialRowKeyScanAssembler} may 
 * be invoked using the current scan context resulting is a precise set of 
 * composite start/stop row keys. These are used in HBase client <a target="#" href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Scan.html">scan</a> API
 * start and stop row.
 * </p>
 * @author Scott Cinnamond
 * @since 0.5
 */
@Deprecated
public class ScanContext extends DefaultQueryVisitor {
    
	private static Log log = LogFactory.getLog(ScanContext.class);
	
	protected PlasmaType rootType;
	protected DataGraphConfig graph;
	protected ScanLiterals partialKeyScanLiterals;
	protected ScanLiterals fuzzyKeyScanLiterals;
	protected boolean hasContiguousPartialKeyScanFieldValues;
	
	protected boolean hasOnlyPartialKeyScanSupportedLogicalOperators = true;
	protected boolean hasOnlyPartialKeyScanSupportedRelationalOperators = true;
	
    @SuppressWarnings("unused")
	private ScanContext() {}

    /**
     * Conducts an initial traversal while capturing and 
     * analyzing the characteristics of a query in order to 
     * leverage the important HBase partial row-key scan
     * capability for every possible predicate expression. 
     * @param rootType the root type
     * @param where the predicates
     */
    public ScanContext(PlasmaType rootType, Where where)
    {
    	this.rootType = rootType;
		QName rootTypeQname = this.rootType.getQualifiedName();
		this.graph = CloudGraphConfig.getInstance().getDataGraph(
				rootTypeQname);
    	if (log.isDebugEnabled())
    		log.debug("begin traverse");
    	
		ScanLiteralAssembler literalAssembler = 
				new ScanLiteralAssembler(this.rootType);
    	where.accept(literalAssembler); // traverse
    	this.partialKeyScanLiterals = literalAssembler.getPartialKeyScanResult();
    	this.fuzzyKeyScanLiterals = literalAssembler.getFuzzyKeyScanResult();
    	
    	where.accept(this); // traverse
    	
    	if (log.isDebugEnabled())
    		log.debug("end traverse");  
    	
    	construct();
    }
    
	private void construct() {
    	if (this.partialKeyScanLiterals.size() == 0)
    		throw new IllegalStateException("no literals found in predicate");
    	this.hasContiguousPartialKeyScanFieldValues = true;
    	int size = this.graph.getUserDefinedRowKeyFields().size();
    	int[] scanLiteralCount = new int[size];
    	
    	for (int i = 0; i < size; i++) {
    		UserDefinedRowKeyFieldConfig fieldConfig = this.graph.getUserDefinedRowKeyFields().get(i); 
    		List<ScanLiteral> list = this.partialKeyScanLiterals.getLiterals(fieldConfig);
    		if (list != null)
    		    scanLiteralCount[i] = list.size();
    		else
    			scanLiteralCount[i] = 0; 
    	}
    	
    	for (int i = 0; i < size-1; i++)
    		if (scanLiteralCount[i] == 0 && scanLiteralCount[i+1] > 0)
    			this.hasContiguousPartialKeyScanFieldValues = false;    	
    }
    
	/**
	 * Return the current scan literals.
	 * @return the current scan literals.
	 */
	public ScanLiterals getPartialKeyScanLiterals() {
		return this.partialKeyScanLiterals;
	}
	
	public ScanLiterals getFuzzyKeyScanLiterals() {
		return this.fuzzyKeyScanLiterals;
	}

	/**
	 * Returns whether an HBase partial row-key scan is possible
	 * under the current scan context. 
	 * @return whether an HBase partial row-key scan is possible
	 * under the current scan context.
	 */
	public boolean canUsePartialKeyScan() {
		//return //this.hasWildcardOperators == false &&
		//	this.hasContiguousPartialKeyScanFieldValues == true &&
		//	this.hasOnlyPartialKeyScanSupportedLogicalOperators == true &&
		//	this.hasOnlyPartialKeyScanSupportedRelationalOperators == true;	
		
		//FIXME: what about OR operator ??
		return this.partialKeyScanLiterals.size() > 0 &&
			this.hasContiguousPartialKeyScanFieldValues;
	}
	
	public boolean canUseFuzzyKeyScan() {
		return this.fuzzyKeyScanLiterals.size() > 0;
	}
	
    /**
     * Returns whether the underlying query predicates represent
     * a contiguous set of composite row-key fields making a partial
     * row-key scan possible. 
     * @return whether the underlying query predicates represent
     * a contiguous set of composite row-key fields making a partial
     * row-key scan possible. 
     */
	public boolean hasContiguousFieldValues() {
		return hasContiguousPartialKeyScanFieldValues;
	}
	
	/**
	 * Returns whether the underlying query contains only
	 * logical operators supportable for under a partial
     * row-key scan.   
	 * @return whether the underlying query contains only
	 * logical operators supportable for under a partial
     * row-key scan.
	 */
	public boolean hasOnlyPartialKeyScanSupportedLogicalOperators() {
		return hasOnlyPartialKeyScanSupportedLogicalOperators;
	} 
	
	/**
	 * Returns whether the underlying query contains only
	 * relational operators supportable for under a partial
     * row-key scan.   
	 * @return whether the underlying query contains only
	 * relational operators supportable for under a partial
     * row-key scan.
	 */
	public boolean hasOnlyPartialKeyScanSupportedRelationalOperators() {
		return hasOnlyPartialKeyScanSupportedRelationalOperators;
	}
	
    /**
     * Process the traversal start event for a query {@link org.plasma.query.model.WildcardOperator WildcardOperator}
     * within an {@link org.plasma.query.model.Expression expression} creating context
     * information useful for determining an HBase scan strategy.
     * @param literal the expression literal
     * @throws GraphServiceException if an unknown wild card operator
     * is encountered.
     */
	public void start(WildcardOperator operator) {
		switch (operator.getValue()) {
		case LIKE:
			break;
		default:
			throw new GraphServiceException("unknown operator '"
					+ operator.getValue().toString() + "'");
		}
		super.start(operator);
	}	
	
    /**
     * Process the traversal start event for a query {@link org.plasma.query.model.LogicalOperator LogicalOperator}
     * within an {@link org.plasma.query.model.Expression expression} creating context
     * information useful for determining an HBase scan strategy.
     * @param literal the expression literal
     * @throws GraphServiceException if an unknown logical operator
     * is encountered.
     */
	public void start(LogicalOperator operator) {
		
		switch (operator.getValue()) {
		case AND:
			break; 
		case OR:	
			// Note: if an OR on 2 fields of the same property
			// 2 partial key scans can be used
			this.hasOnlyPartialKeyScanSupportedLogicalOperators = false;
			break;
		}
		super.start(operator);
	}
    
    /**
     * Process the traversal start event for a query {@link org.plasma.query.model.RelationalOperator RelationalOperator}
     * within an {@link org.plasma.query.model.Expression expression} creating context
     * information useful for determining an HBase scan strategy.
     * @param literal the expression literal
     * @throws GraphServiceException if an unknown relational operator
     * is encountered.
     */
	public void start(RelationalOperator operator) {
		switch (operator.getValue()) {
		case EQUALS:
		case GREATER_THAN:
		case GREATER_THAN_EQUALS:
		case LESS_THAN:
		case LESS_THAN_EQUALS:
			break;
		case NOT_EQUALS: 
			// partial key scan is a range of keys. Not-equals is therefore not applicable
			this.hasOnlyPartialKeyScanSupportedRelationalOperators = false;
			break;
		default:
			throw new QueryException("unknown operator '"
					+ operator.getValue().toString() + "'");
		}
		super.start(operator);
	}
	
    /**
     * Process the traversal start event for a query {@link org.plasma.query.model.GroupOperator GroupOperator}
     * within an {@link org.plasma.query.model.Expression expression} creating context
     * information useful for determining an HBase scan strategy.
     * @param literal the expression literal
     * @throws GraphServiceException if an unknown group operator
     * is encountered.
     */
	public void start(GroupOperator operator) {
		switch (operator.getValue()) {
		case RP_1:  		
	        break;
		case RP_2:  		
		    break;
		case RP_3:  			
			break;
		case LP_1:  
			break;
		case LP_2:  			
			break;
		case LP_3:  
			break;
		default:
			throw new QueryException("unknown group operator, "
						+ operator.getValue().name());
		}
		super.start(operator);
	}
}
