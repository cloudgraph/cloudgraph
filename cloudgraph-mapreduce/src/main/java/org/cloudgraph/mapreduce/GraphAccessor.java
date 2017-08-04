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
package org.cloudgraph.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.plasma.query.Query;

import commonj.sdo.DataGraph;

/**
 * Supports access to arbitrary data graphs at various stages of a
 * <code>Job</code> as supplied based on the given <a
 * href="http://plasma-sdo.org/org/plasma/query/Query.html">query</a>.
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 * 
 * @see GraphWritable
 * @see GraphMutator
 */
public interface GraphAccessor {
  public DataGraph[] find(Query query, JobContext context) throws IOException;
}
