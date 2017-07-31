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
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Marshalling context which uses new commons pool2 for pooling of non
 * validating data binding instances.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 * 
 * @see StateNonValidatingDataBinding
 * @see StateMarshalingContext
 */
public class PooledStateMarshallingContext implements StateMarshalingContext {
  private static Log log = LogFactory.getLog(PooledStateMarshallingContext.class);
  private GenericObjectPool<StateNonValidatingDataBinding> pool;

  @SuppressWarnings("unused")
  private PooledStateMarshallingContext() {
  }

  public PooledStateMarshallingContext(GenericObjectPoolConfig config,
      StateDataBindingFactory factory) {
    if (log.isDebugEnabled())
      log.debug("initializing data binding pool...");
    this.pool = new GenericObjectPool<StateNonValidatingDataBinding>(factory);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.cloudgraph.state.StateMarshalingContext#getBinding()
   */
  @Override
  public NonValidatingDataBinding getBinding() {
    try {
      return this.pool.borrowObject();
    } catch (Exception e) {
      throw new StateException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.cloudgraph.state.StateMarshalingContext#returnDataBinding(org.cloudgraph
   * .state.NonValidatingDataBinding)
   */
  @Override
  public void returnBinding(NonValidatingDataBinding binding) {
    this.pool.returnObject((StateNonValidatingDataBinding) binding);
  }

}
