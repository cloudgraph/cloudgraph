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
package org.cloudgraph.store.service;

import java.util.Collection;

/**
 * Encapsulates the collection resulting from query execution along with the
 * Query object itself. Hopefully usefull where the query construction is
 * seperated off and the results are a collection rather than a single object.
 * In this event after collection iteration is complete, the query must be
 * closed via. Query.closeAll(), Query.close(), Extent.close(Iterator), or sume
 * such JDO call which frees appropriate re sources associated with query
 * execution.
 *
 */
public class QueryResults {
  private Collection collection;

  private QueryResults() {
  }

  public QueryResults(Collection collection) {
    this.collection = collection;
  }

  public Collection getResults() {
    return collection;
  }

}
