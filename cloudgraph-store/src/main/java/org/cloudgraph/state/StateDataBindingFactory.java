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
package org.cloudgraph.state;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * The data binding class <@link StateNonValidatingDataBinding> is both not thread safe and 
 * slow on creation due to the underlying JAXP XML Schema parsing, and therefore
 * this class provides a factory implementation for the associated binding pool
 * in support of concurrent contexts.
 *  
 * @author Scott Cinnamond
 * @since 0.6.2
 * 
 * @see StateNonValidatingDataBinding
 */
public class StateDataBindingFactory extends BasePooledObjectFactory<StateNonValidatingDataBinding> {

	@Override
	public StateNonValidatingDataBinding create() throws Exception {
		return new StateNonValidatingDataBinding();
	}

	@Override
	public PooledObject<StateNonValidatingDataBinding> wrap(
			StateNonValidatingDataBinding binding) {
		return new DefaultPooledObject<StateNonValidatingDataBinding>(binding);	
	}
}
