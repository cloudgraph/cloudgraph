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
package org.cloudgraph.hbase.service;

import org.cloudgraph.state.StateMarshalingContext;

/**
 * Holds temporary state information for use within services held for the
 * duration of a service call. For most implementations the <{@link close} method
 * must be called within the finally block of the service method to free associated 
 * resources.
 * 
 * @author Scott Cinnamond
 * @since 0.6.3
 */
public interface ServiceContext {
  
	/** 
	 * Returns the state marshalling context associated with the context. 
	 * @return the state marshalling context associated with the context.
	 */
	public StateMarshalingContext getMarshallingContext();
    
	/**
	 * Frees any resources associated with this context. 
	 */
    public void close();
}
