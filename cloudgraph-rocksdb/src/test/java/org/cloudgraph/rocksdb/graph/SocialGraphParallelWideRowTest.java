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
package org.cloudgraph.rocksdb.graph;

import java.io.IOException;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rocksdb.test.SocialGraphModelTest;
import org.cloudgraph.store.mapping.FetchType;
import org.cloudgraph.store.mapping.ParallelFetchDisposition;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.cloudgraph.test.socialgraph.actor.Topic;
import org.cloudgraph.test.socialgraph.actor.query.QActor;
import org.cloudgraph.test.socialgraph.story.Blog;
import org.plasma.common.test.PlasmaTestSetup;
import org.plasma.query.Query;
import org.plasma.query.model.ConfigurationProperty;

import commonj.sdo.DataGraph;

/**
 * Tests parallel query operations on a graph, where the graph is stored as a
 * single "wide" row as opposed to being distributed across HBase tables. See
 * the associated *cloudgraph-config.xml which maps only the social graph
 * {@link Actor} root to a table, the remaining graph modes being "unbound" and
 * therefore part of the wide row.
 * 
 * @author Scott Cinnamond
 * @since 1.0.7
 */
public class SocialGraphParallelWideRowTest extends SocialGraphModelTest {
  private static Log log = LogFactory.getLog(SocialGraphParallelWideRowTest.class);

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(SocialGraphParallelWideRowTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testInsert() throws IOException {
    long timestamp = System.nanoTime();
    String namePrefix = "ac" + String.valueOf(timestamp).substring(11) + "_";

    GraphInfo[] graphs = new GraphInfo[3];
    graphs[0] = createGraph(namePrefix);
    graphs[1] = createGraph(namePrefix);
    graphs[2] = createGraph(namePrefix);

    log.debug("inserting initial graphs");
    this.service.commit(
        new DataGraph[] { graphs[0].actor.getDataGraph(), graphs[1].actor.getDataGraph(),
            graphs[2].actor.getDataGraph() }, "test1");

    QActor query = createGraphQuery(namePrefix + "*");
    parallelize(query);

    log.debug("fetching initial graphs");
    Actor[] actors = this.fetchGraphs(query);
    int i = 0;
    for (Actor fetched : actors) {
      String xml = this.serializeGraph(fetched.getDataGraph());
      log.debug(xml);

      assertTrue(fetched.getTargetEdgeCount() == 1);
      Actor fetchedFollower = (Actor) fetched.getTargetEdge(0).getSource();
      assertTrue(fetchedFollower.getName() != null);

      assertTrue(fetched.getBlogCount() == 2);
      Blog fetchedBlog = (Blog) fetched.get("blog[@name='" + graphs[i].electionBlog.getName()
          + "']");
      assertTrue(fetchedBlog != null);
      assertTrue(fetchedBlog.getTopicCount() == 1);
      Topic fetchedTopic = fetchedBlog.getTopic(0);
      assertTrue(fetchedTopic.getName() != null);
      assertTrue(fetchedTopic.getName().equals(graphs[i].politics.getName()));

      log.debug("fetching follower");
      Actor fetchedFollowerRoot = fetchGraph(createFollowerGraphQuery(graphs[i].follower.getName()));
      xml = this.serializeGraph(fetchedFollowerRoot.getDataGraph());
      log.debug(xml);
      // Since actor is a "bound" root type, there are always
      // two actor rows, and the edge between
      // actor and follower is created in the first row.
      // Because edge and its derivatives are not "bound"
      // we cannot traverse from the follower back through the edge to
      // the actor. If the edge becomes a bound root, we could.
      assertTrue(fetchedFollowerRoot != null);
      assertTrue(fetchedFollowerRoot.getSourceEdgeCount() == 0);
      i++;
    }

  }

  private void parallelize(Query query) {
    ConfigurationProperty parallel = new ConfigurationProperty();
    parallel
        .setName(org.cloudgraph.store.mapping.ConfigurationProperty.CLOUDGRAPH___QUERY___FETCHTYPE
            .value());
    parallel.setValue(FetchType.PARALLEL.value());
    query.getModel().getConfigurationProperties().add(parallel);

    ConfigurationProperty disposition = new ConfigurationProperty();
    disposition
        .setName(org.cloudgraph.store.mapping.ConfigurationProperty.CLOUDGRAPH___QUERY___PARALLELFETCH___DISPOSITION
            .value());
    disposition.setValue(ParallelFetchDisposition.TALL.value());
    query.getModel().getConfigurationProperties().add(disposition);

  }

}
