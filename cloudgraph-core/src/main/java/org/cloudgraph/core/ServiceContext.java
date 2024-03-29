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
package org.cloudgraph.core;

import org.cloudgraph.core.client.ClientFactory;
import org.cloudgraph.core.filter.ColumnFilterFactory;
import org.cloudgraph.core.filter.RowFilterFactory;
import org.cloudgraph.core.scan.RowKeyFactory;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.sdo.access.AccessServiceContext;

/**
 * Holds temporary state information for use within services held for the
 * duration of a service call. For most implementations the <{@link close}
 * method must be called within the finally block of the service method to free
 * associated resources.
 * 
 * @author Scott Cinnamond
 * @since 0.6.3
 */
public interface ServiceContext extends AccessServiceContext {
  public StoreMappingContext getStoreMapping();

  public DataConverter getDataConverter();

  public ConnectionManager getConnectionManager();

  public ClientFactory getClientFactory();

  public RowKeyFactory getRowKeyFactory();

  public ColumnFilterFactory getColumnFilterFactory();

  public RowFilterFactory getRowFilterFactory();

  public String getNamespaceQualifiedPhysicalName(TableMapping tableConfig,
      StoreMappingContext storeMapping);
}
