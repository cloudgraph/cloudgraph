/**
 * Copyright 2017 TerraMeta Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
      this.marshallingContext = new SimpleStateMarshallingContext(this.pool.getBinding());
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
