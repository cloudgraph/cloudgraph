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

import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * Assembles a complete composite or partial composite partial row (start/stop) key pair where each
 * field within the composite start and stop row keys are constructed 
 * based a set of query predicates.   
 * @author Scott Cinnamond
 * @since 0.5
 */
public interface RowKeyScanAssembler {
    /**
     * Assemble row key scan information based on one or more
     * given query predicates.
     * @param where the where predicate hierarchy
     * @param contextType the context type which may be the root type or another
     * type linked by one or more relations to the root
     */
	public void assemble(Where where, PlasmaType contextType);
    
	/**
     * Assemble row key scan information based only on the
     * data graph root type information such as the URI
     * and type logical or physical name. 
     */
	public void assemble();
	
	
	/**
	 * Assemble row key scan information based on the given
	 * scan literals.
	 * @param literals the scan literals
	 */
	public void assemble(ScanLiterals literals);
}
