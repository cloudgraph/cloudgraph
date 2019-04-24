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
package org.cloudgraph.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.cloudgraph.mapreduce.GraphAccessor;
import org.cloudgraph.mapreduce.GraphMutator;
import org.plasma.query.Query;

import commonj.sdo.DataGraph;

/**
 *
 * @param <KEYIN>
 *          The type of the input key.
 * @param <VALUEIN>
 *          The type of the input value.
 * @param <KEYOUT>
 *          The type of the output key.
 * @see org.apache.hadoop.mapreduce.Reducer
 */
public class GraphReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Writable>
/* implements GraphMutator, GraphAccessor */{
  // private GraphServiceDelegate serviceDelegate;

  public GraphReducer() {
    // this.serviceDelegate = new GraphServiceDelegate();
  }

  // @Override
  // public DataGraph[] find(Query query, JobContext context) throws IOException
  // {
  // return this.serviceDelegate.find(query, context);
  // }
  //
  // @Override
  // public void commit(DataGraph graph, JobContext context) throws IOException
  // {
  // this.serviceDelegate.commit(graph, context);
  // }
  //
  // @Override
  // public void commit(DataGraph[] graphs, JobContext context) throws
  // IOException {
  // this.serviceDelegate.commit(graphs, context);
  // }

}