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
    context = new PooledStateMarshallingContext(config, new StateDataBindingFactory());
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
