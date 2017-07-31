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
    implements GraphMutator, GraphAccessor {
  private GraphServiceDelegate serviceDelegate;

  public GraphReducer() {
    this.serviceDelegate = new GraphServiceDelegate();
  }

  @Override
  public DataGraph[] find(Query query, JobContext context) throws IOException {
    return this.serviceDelegate.find(query, context);
  }

  @Override
  public void commit(DataGraph graph, JobContext context) throws IOException {
    this.serviceDelegate.commit(graph, context);
  }

  @Override
  public void commit(DataGraph[] graphs, JobContext context) throws IOException {
    this.serviceDelegate.commit(graphs, context);
  }

}