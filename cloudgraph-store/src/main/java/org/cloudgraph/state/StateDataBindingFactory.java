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

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * The data binding class <@link StateNonValidatingDataBinding> is both not
 * thread safe and slow on creation due to the underlying JAXP XML Schema
 * parsing, and therefore this class provides a factory implementation for the
 * associated binding pool in support of concurrent contexts.
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
  public PooledObject<StateNonValidatingDataBinding> wrap(StateNonValidatingDataBinding binding) {
    return new DefaultPooledObject<StateNonValidatingDataBinding>(binding);
  }
}
