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
import java.util.UUID;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.test.SocialGraphModelTest;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.cloudgraph.test.socialgraph.actor.Photo;
import org.cloudgraph.test.socialgraph.actor.query.QActor;
import org.plasma.common.test.PlasmaTestSetup;
import org.plasma.query.model.FunctionName;

import commonj.sdo.DataGraph;
import commonj.sdo.Property;

/**
 * Tests scan operations
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class SocialGraphAggregatesTest extends SocialGraphModelTest {
  private static Log log = LogFactory.getLog(SocialGraphAggregatesTest.class);

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(SocialGraphAggregatesTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testSimpleGroupByHaving() throws IOException {

    String prefix = String.valueOf(System.nanoTime()).substring(10);

    Actor larry = this.createRootActor(prefix + "larry");
    larry.setAge(55);

    Actor curly = this.createRootActor(prefix + "curly");
    curly.setAge(65);

    Actor moe = this.createRootActor(prefix + "moe");
    moe.setAge(70);

    Actor shemp = this.createRootActor(prefix + "shemp");
    shemp.setAge(65);

    this.service.commit(
        new DataGraph[] { larry.getDataGraph(), curly.getDataGraph(), moe.getDataGraph(),
            shemp.getDataGraph() }, "stooges");

    QActor root = QActor.newQuery();
    root.select(FunctionName.COUNT, root.id());
    root.select(root.age().avg());
    root.select(root.age().min());
    root.select(root.age().max());

    root.where(root.name().like(prefix + "*"));
    root.orderBy(root.age());
    root.groupBy(root.age());
    root.having(root.age().gt(60));

    log.info(this.marshal(root.getModel()));

    log.debug("fetching initial graphs");
    Actor[] fetchedActors = fetchGraphs(root);
    assertTrue(fetchedActors != null);
    assertTrue(fetchedActors.length == 2); // 2 aggregates
    for (Actor actor : fetchedActors) {
      // xml = this.serializeGraph(actor.getDataGraph());
      // log.info(xml);
      log.info(actor.dump());
    }
    Actor agg1 = fetchedActors[0];
    Property idProperty = agg1.getType().getProperty(Actor.ID);
    Long count = (Long) agg1.get(FunctionName.COUNT, idProperty);
    assertTrue(count != null);
    assertTrue(count.longValue() == 2); // 2 with age 65

    Actor agg2 = fetchedActors[1];
    count = (Long) agg2.get(FunctionName.COUNT, idProperty);
    assertTrue(count != null);
    assertTrue(count.longValue() == 1); // 2 with age 70
  }

  public void testGraphGroupByHaving() throws IOException {

    String prefix = String.valueOf(System.nanoTime()).substring(10);

    Actor larry = this.createRootActor(prefix + "larry");
    larry.setAge(55);
    Photo air = larry.createPhoto();
    air.setName("air");
    air.setId(UUID.randomUUID().toString());
    air.setDescription("a photo of some air");
    air.setContent(air.getDescription().getBytes());

    Actor curly = this.createRootActor(prefix + "curly");
    curly.setAge(60);
    Photo land = curly.createPhoto();
    land.setName("land");
    land.setId(UUID.randomUUID().toString());
    land.setDescription("a photo of land");
    land.setContent(land.getDescription().getBytes());

    Actor moe = this.createRootActor(prefix + "moe");
    moe.setAge(60);
    Photo sea = moe.createPhoto();
    sea.setName("sea");
    sea.setId(UUID.randomUUID().toString());
    sea.setDescription("a photo of sea");
    sea.setContent(sea.getDescription().getBytes());

    Photo air2 = moe.createPhoto();
    air2.setName("air");
    air2.setId(UUID.randomUUID().toString());
    air2.setDescription("a photo of some air");
    air2.setContent(air2.getDescription().getBytes());

    this.service.commit(
        new DataGraph[] { larry.getDataGraph(), curly.getDataGraph(), moe.getDataGraph() },
        "stooges");

    QActor query = QActor.newQuery();
    query.select(FunctionName.COUNT, query.id());

    query.where(query.name().like(prefix + "*"));
    query.orderBy(query.age());
    query.groupBy(query.age());
    query.having(query.photo().name().eq("air").or(query.photo().name().eq("land")));

    log.info(this.marshal(query.getModel()));

    log.debug("fetching initial graphs");
    Actor[] fetchedActors = fetchGraphs(query);
    assertTrue(fetchedActors != null);
    assertTrue(fetchedActors.length == 2); // 2 aggregates
    for (Actor actor : fetchedActors) {
      // xml = this.serializeGraph(actor.getDataGraph());
      // log.info(xml);
      log.info(actor.dump());
    }

  }
}
