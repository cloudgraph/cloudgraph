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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rocksdb.test.SocialGraphModelTest;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.cloudgraph.test.socialgraph.actor.Photo;
import org.cloudgraph.test.socialgraph.actor.query.QActor;
import org.plasma.common.test.PlasmaTestSetup;
import org.plasma.query.model.FunctionName;

import commonj.sdo.DataGraph;
import commonj.sdo.Property;

/**
 * Tests aggregate operations
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

  public void testSimpleGroupBy() throws IOException {

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
    root.groupBy(root.age());

    log.info(this.marshal(root.getModel()));

    log.debug("fetching initial graphs");
    Actor[] fetchedActors = fetchGraphs(root);
    assertTrue(fetchedActors != null);
    assertTrue(fetchedActors.length == 3);
    for (Actor actor : fetchedActors) {
      // xml = this.serializeGraph(actor.getDataGraph());
      // log.info(xml);
      log.info(actor.dump());
    }

    Property idProperty = fetchedActors[0].getType().getProperty(Actor.ID);
    List<Actor> countTwo = findCount(2, fetchedActors, idProperty);
    assertTrue(countTwo != null);
    assertTrue(countTwo.size() == 1);

    List<Actor> countOne = findCount(1, fetchedActors, idProperty);
    assertTrue(countOne != null);
    assertTrue(countOne.size() == 2);
  }

  private List<Actor> findCount(long count, Actor[] actors, Property prop) {
    List<Actor> result = null;
    for (Actor actor : actors) {
      Long cnt = (Long) actor.get(FunctionName.COUNT, prop);
      if (cnt != null && cnt.longValue() == count) {
        if (result == null)
          result = new ArrayList<Actor>();
        result.add(actor);
      }
    }
    return result;
  }

  public void testSimpleGroupByHaving() throws IOException {

    String prefix = String.valueOf(System.nanoTime()).substring(10);

    Actor larry = this.createRootActor(prefix + "larry");
    larry.setAge(55);
    larry.setIq(99);
    Photo air = larry.createPhoto();
    air.setName("air");
    air.setId(UUID.randomUUID().toString());
    air.setDescription("a photo of some air");
    air.setContent(air.getDescription().getBytes());

    Actor curly = this.createRootActor(prefix + "curly");
    curly.setAge(60);
    curly.setIq(122);
    Photo land = curly.createPhoto();
    land.setName("land");
    land.setId(UUID.randomUUID().toString());
    land.setDescription("a photo of land");
    land.setContent(land.getDescription().getBytes());

    Actor moe = this.createRootActor(prefix + "moe");
    moe.setAge(60);
    moe.setIq(85);
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

    Actor shemp = this.createRootActor(prefix + "shemp");
    shemp.setAge(65);
    shemp.setIq(82);

    this.service.commit(
        new DataGraph[] { larry.getDataGraph(), curly.getDataGraph(), moe.getDataGraph() },
        "stooges");

    QActor query = QActor.newQuery();
    query.select(query.age().count()).select(query.age().avg()).select(query.age().min())
        .select(query.age().max());

    query.where(query.name().like(prefix + "*"));
    query.orderBy(query.age());
    query.groupBy(query.age());
    query.having(query.age().avg().gt(50));

    log.info(this.marshal(query.getModel()));

    log.debug("fetching initial graphs");
    Actor[] fetchedActors = fetchGraphs(query);
    assertTrue(fetchedActors != null);
    assertTrue(fetchedActors.length == 1); // 1 aggregates
    for (Actor actor : fetchedActors) {
      // xml = this.serializeGraph(actor.getDataGraph());
      // log.info(xml);
      log.info(actor.dump());
    }

  }
}
