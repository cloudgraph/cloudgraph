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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.state.PooledStateManager;
import org.cloudgraph.state.SimpleStateMarshallingContext;
import org.cloudgraph.state.StateMarshalingContext;

/**
 * Uses a global pool for managing {@link StateMarshalingContext} and only
 * creates an instance on demand, returning the instance to the pool on {link
 * ServiceContext.close}.
 * 
 * @see StateMarshalingContext
 * @see PooledStateManager
 * @author Scott Cinnamond
 * @since 0.6.3
 */
public class LazyServiceContext implements ServiceContext {
	private static Log log = LogFactory.getLog(LazyServiceContext.class);
	private StateMarshalingContext marshallingContext;
	private PooledStateManager pool;

	@SuppressWarnings("unused")
	private LazyServiceContext() {
	}
	public LazyServiceContext(PooledStateManager pooledMarshallingContext) {
		this.pool = pooledMarshallingContext;
	}

	@Override
	public StateMarshalingContext getMarshallingContext() {
		if (this.marshallingContext == null) {
			if (log.isDebugEnabled())
				log.debug("getting marshalling context from pool");
			this.marshallingContext = new SimpleStateMarshallingContext(
					this.pool.getBinding());
		}
		return this.marshallingContext;
	}

	@Override
	public void close() {
		if (this.marshallingContext != null) {
			if (log.isDebugEnabled())
				log.debug("returning marshalling context to pool");
			this.pool.returnBinding(this.marshallingContext.getBinding());
			this.marshallingContext = null;
		}
	}
}
