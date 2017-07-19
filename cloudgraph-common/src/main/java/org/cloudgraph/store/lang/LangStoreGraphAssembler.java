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
package org.cloudgraph.store.lang;

import java.util.List;

import org.plasma.sdo.access.DataGraphAssembler;
import org.plasma.sdo.access.provider.common.PropertyPair;

/**
 * A graph assembler specific to query language driven data stores.
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public interface LangStoreGraphAssembler extends DataGraphAssembler {
	/**
	 * Initiates the assembly of a data graph based on the 
	 * given property key value pair results list. 
	 * @param results the results list
	 * 
	 * @see DataGraphAssembler.getDataGraph()
	 */
	public void assemble(List<PropertyPair> results);
}
