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
package org.cloudgraph.hbase.graph;

import java.io.IOException;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.test.SocialGraphModelTest;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.plasma.common.test.PlasmaTestSetup;

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
    GraphInfo graph1 = createSimpleGraph();
    String xml = this.serializeGraph(graph1.actor.getDataGraph());
    log.debug("inserting graph1:");
    log.debug(xml);
    this.service.commit(graph1.actor.getDataGraph(), "test1");

    GraphInfo graph2 = createSimpleGraph();
    xml = this.serializeGraph(graph2.actor.getDataGraph());
    log.debug("inserting graph2:");
    log.debug(xml);
    this.service.commit(graph2.actor.getDataGraph(), "test2");

    GraphInfo graph3 = createSimpleGraph();
    xml = this.serializeGraph(graph3.actor.getDataGraph());
    log.debug("inserting graph3:");
    log.debug(xml);
    this.service.commit(graph3.actor.getDataGraph(), "test3");

    log.debug("fetching initial graphs");
    Actor[] fetchedActors = fetchGraphs(createTopicScanQuery(graph1.actor.getName(),
        graph2.actor.getName(), graph3.actor.getName(), graph1.weather, graph1.politics));
    assertTrue(fetchedActors != null && fetchedActors.length == 3);
    for (Actor actor : fetchedActors) {
      xml = this.serializeGraph(actor.getDataGraph());
      log.debug(xml);
    }

    /*
     * fetchedActor = fetchGraph(
     * createTopicWildcardScanQuery(graph1.actor.getName())); xml =
     * this.serializeGraph(fetchedActor.getDataGraph()); log.debug(xml);
     */

  }

}
