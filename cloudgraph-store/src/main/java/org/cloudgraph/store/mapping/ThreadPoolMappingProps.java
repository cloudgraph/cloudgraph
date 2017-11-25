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
package org.cloudgraph.store.mapping;

import org.cloudgraph.store.mapping.FetchType;
import org.cloudgraph.store.mapping.ParallelFetchDisposition;
import org.plasma.query.model.Query;

/**
 * Captures a few very important concurrency related properties.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public class ThreadPoolMappingProps {
  private int minThreadPoolSize;
  private int maxThreadPoolSize;
  private int maxThreadDepth;
  private FetchType fetchType;
  private ParallelFetchDisposition fetchDisposition;

  public ThreadPoolMappingProps(Query query) {
    this.minThreadPoolSize = StoreMappingProp.getQueryPoolMin(query);
    this.maxThreadPoolSize = StoreMappingProp.getQueryPoolMax(query);
    if (this.minThreadPoolSize > this.maxThreadPoolSize)
      this.minThreadPoolSize = this.maxThreadPoolSize;
    this.maxThreadDepth = StoreMappingProp.getQueryThreadMaxDepth(query);
    this.fetchType = StoreMappingProp.getQueryFetchType(query);
    this.fetchDisposition = StoreMappingProp.getQueryParallelFetchDisposition(query);
  }

  @Deprecated
  public ThreadPoolMappingProps(int minThreadPoolSize, int maxThreadPoolSize, int maxThreadDepth) {
    super();
    this.minThreadPoolSize = minThreadPoolSize;
    this.maxThreadPoolSize = maxThreadPoolSize;
    this.maxThreadDepth = maxThreadDepth;
  }

  public ParallelFetchDisposition getFetchDisposition() {
    return fetchDisposition;
  }

  public FetchType getFetchType() {
    return fetchType;
  }

  public int getMinThreadPoolSize() {
    return minThreadPoolSize;
  }

  public int getMaxThreadPoolSize() {
    return maxThreadPoolSize;
  }

  public int getMaxThreadDepth() {
    return maxThreadDepth;
  }
}
