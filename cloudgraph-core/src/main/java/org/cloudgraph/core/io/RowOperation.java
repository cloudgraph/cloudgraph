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
package org.cloudgraph.core.io;

import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.state.RowState;
import org.cloudgraph.store.mapping.StoreMappingContext;

/**
 * Represents the operational aspects of a statefull graph row.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface RowOperation extends RowState {
  public ServiceContext getServiceContext();
}
