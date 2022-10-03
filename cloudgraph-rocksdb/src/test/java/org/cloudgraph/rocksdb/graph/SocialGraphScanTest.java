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
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.plasma.common.test.PlasmaTestSetup;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaDataGraphVisitor;

import commonj.sdo.DataObject;

/**
 * Tests scan operations
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class SocialGraphScanTest extends SocialGraphModelTest {
  private static Log log = LogFactory.getLog(SocialGraphScanTest.class);

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(SocialGraphScanTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testInsert() throws IOException {
    String namePrefix = String.valueOf(System.nanoTime()).substring(10);
    namePrefix = namePrefix + "social_";

    GraphInfo graph1 = createGraph(namePrefix);
    String xml = this.serializeGraph(graph1.actor.getDataGraph());
    log.debug("inserting graph1:");
    log.debug(xml);
    this.service.commit(graph1.actor.getDataGraph(), "test1");
    Actor[] fetchedActors = fetchGraphs(this.createActorBlogGraphQuery(graph1.actor.getName(),
        graph1.actor.getId()));
    assertTrue(fetchedActors != null);
    assertTrue(fetchedActors.length >= 1);
    for (Actor actor : fetchedActors) {
      xml = this.serializeGraph(actor.getDataGraph());
      log.debug(xml);
      assertTrue("unexpected name: " + actor.getName(),
          actor.getName().equals(graph1.actor.getName()));
      assertTrue("unexpected id: " + actor.getId(), actor.getId().equals(graph1.actor.getId()));
    }
    assertTrue("unexpected " + String.valueOf(fetchedActors.length), fetchedActors.length == 1);

    GraphInfo graph2 = createGraph(namePrefix);
    xml = this.serializeGraph(graph2.actor.getDataGraph());
    log.debug("inserting graph2:");
    log.debug(xml);
    this.service.commit(graph2.actor.getDataGraph(), "test2");
    fetchedActors = fetchGraphs(this.createActorBlogGraphQuery(graph2.actor.getName(),
        graph2.actor.getId()));
    assertTrue(fetchedActors != null);
    assertTrue(fetchedActors.length >= 1);
    for (Actor actor : fetchedActors) {
      xml = this.serializeGraph(actor.getDataGraph());
      log.debug(xml);
      assertTrue("unexpected name: " + actor.getName(),
          actor.getName().equals(graph2.actor.getName()));
      assertTrue("unexpected id: " + actor.getId(), actor.getId().equals(graph2.actor.getId()));
    }
    assertTrue("unexpected " + String.valueOf(fetchedActors.length), fetchedActors.length == 1);

    GraphInfo graph3 = createGraph(namePrefix);
    xml = this.serializeGraph(graph3.actor.getDataGraph());
    log.debug("inserting graph3:");
    log.debug(xml);
    this.service.commit(graph3.actor.getDataGraph(), "test3");
    fetchedActors = fetchGraphs(this.createActorBlogGraphQuery(graph3.actor.getName(),
        graph3.actor.getId()));
    assertTrue(fetchedActors != null);
    assertTrue(fetchedActors.length >= 1);
    for (Actor actor : fetchedActors) {
      xml = this.serializeGraph(actor.getDataGraph());
      log.debug(xml);
      assertTrue("unexpected name: " + actor.getName(),
          actor.getName().equals(graph3.actor.getName()));
      assertTrue("unexpected id: " + actor.getId(), actor.getId().equals(graph3.actor.getId()));
    }
    assertTrue("unexpected " + String.valueOf(fetchedActors.length), fetchedActors.length == 1);

    log.debug("fetching initial graphs");

    // fetchedActors = fetchGraphs(createGraphQuery(namePrefix + "*"));
    fetchedActors = fetchGraphs(createActorBlogGraphQuery(namePrefix + "*"));

    assertTrue(fetchedActors != null);
    for (Actor actor : fetchedActors) {
      PlasmaDataGraph graph = (PlasmaDataGraph) actor.getDataGraph();
      xml = this.serializeGraph(actor.getDataGraph());
      log.debug(xml);
    }
    assertTrue("unexpected " + String.valueOf(fetchedActors.length), fetchedActors.length == 3);

    /*
     * fetchedActor = fetchGraph(
     * createTopicWildcardScanQuery(graph1.actor.getName())); xml =
     * this.serializeGraph(fetchedActor.getDataGraph()); log.debug(xml);
     */

  }

}
