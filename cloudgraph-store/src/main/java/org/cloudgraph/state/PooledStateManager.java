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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * A singleton for pooled JAXB (schema) state marshalling context objects.
 * 
 * @see PooledStateMarshallingContext
 * @see StateDataBindingFactory
 * @see NonValidatingDataBinding
 * 
 * @author Scott Cinnamond
 * @since 0.6.3
 */
public class PooledStateManager implements StateMarshalingContext {
	private static Log log = LogFactory.getLog(PooledStateManager.class);
	private static volatile PooledStateManager instance = null;
	private PooledStateMarshallingContext context;

	private PooledStateManager() {
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxTotal(40);
		config.setMinIdle(40);
		context = new PooledStateMarshallingContext(config,
				new StateDataBindingFactory());
	}

	public static PooledStateManager getInstance() {
		if (instance == null)
			initializeInstance();
		return instance;
	}

	private static synchronized void initializeInstance() {
		if (instance == null)
			instance = new PooledStateManager();
	}

	@Override
	public NonValidatingDataBinding getBinding() {
		return context.getBinding();
	}

	@Override
	public void returnBinding(NonValidatingDataBinding binding) {
		context.returnBinding(binding);
	}

}
