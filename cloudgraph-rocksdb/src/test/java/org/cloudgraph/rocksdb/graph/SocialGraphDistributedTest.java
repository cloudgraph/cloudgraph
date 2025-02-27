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
import java.util.UUID;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rocksdb.test.SocialGraphModelTest;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.cloudgraph.test.socialgraph.actor.Photo;
import org.cloudgraph.test.socialgraph.actor.Topic;
import org.cloudgraph.test.socialgraph.story.Blog;
import org.plasma.common.test.PlasmaTestSetup;

/**
 * Tests operations on a distributed graph.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class SocialGraphDistributedTest extends SocialGraphModelTest {
  private static Log log = LogFactory.getLog(SocialGraphDistributedTest.class);

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(SocialGraphDistributedTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testInsert() throws IOException {
    GraphInfo info = createGraph();

    String xml = this.serializeGraph(info.actor.getDataGraph());
    log.debug("inserting initial graph:");
    log.debug(xml);
    this.service.commit(info.actor.getDataGraph(), "test1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    assertTrue(fetchedActor.getTargetEdgeCount() == 1);
    Actor fetchedFollower = (Actor) fetchedActor.getTargetEdge(0).getSource();
    assertTrue(fetchedFollower.getName() != null);
    assertTrue(info.follower.getName().equals(fetchedFollower.getName()));
    assertTrue(fetchedActor.getBlogCount() == 2);
    Blog fetchedBlog = (Blog) fetchedActor.get("blog[@name='" + info.electionBlog.getName() + "']");
    assertTrue(fetchedBlog != null);
    assertTrue(fetchedBlog.getTopicCount() == 1);
    Topic fetchedTopic = fetchedBlog.getTopic(0);
    assertTrue(fetchedTopic.getName() != null);
    assertTrue(fetchedTopic.getName().equals(info.politics.getName()));

    log.debug("fetching follower graph");
    Actor fetchedFollowerRoot = fetchGraph(createFollowerGraphQuery(info.follower.getName()));
    xml = this.serializeGraph(fetchedFollowerRoot.getDataGraph());
    log.debug(xml);
    // Since actor is a "bound" root type, there are always
    // two actor rows, and the edge between
    // actor and follower is created in the first/parent row.
    // Because edge and its derivatives are not "bound"
    // we cannot traverse from the follower back through the edge to
    // the actor. If the edge becomes a bound root, we could.
    assertTrue(fetchedFollowerRoot != null);
    assertTrue(fetchedFollowerRoot.getSourceEdgeCount() == 0);

    log.debug("fetching blog slice");
    Actor fetchedActorSliceRoot = fetchGraph(createBlogPredicateQuery(info.actor.getName(),
        info.electionBlog.getName()));
    xml = this.serializeGraph(fetchedActorSliceRoot.getDataGraph());
    log.debug(xml);
    assertTrue(fetchedActorSliceRoot.getBlogCount() == 1);
    fetchedBlog = (Blog) fetchedActorSliceRoot.get("blog[@name='" + info.electionBlog.getName()
        + "']");
    assertTrue(fetchedBlog != null);
    assertTrue(fetchedActor.getTargetEdgeCount() == 1);
    fetchedFollower = (Actor) fetchedActor.getTargetEdge(0).getSource();
    assertTrue(fetchedFollower.getName() != null);
    assertTrue(info.follower.getName().equals(fetchedFollower.getName()));

    log.debug("fetching photo slice");
    fetchedActorSliceRoot = fetchGraph(createPhotoPredicateQuery(info.actor.getName(),
        info.photo2.getName()));
    xml = this.serializeGraph(fetchedActorSliceRoot.getDataGraph());
    log.debug(xml);
    assertTrue(fetchedActorSliceRoot.getPhotoCount() == 1);
    Photo fetchedPhoto = (Photo) fetchedActorSliceRoot.get("photo[@name='" + info.photo2.getName()
        + "']");
    assertTrue(fetchedPhoto != null);
    assertTrue(fetchedActor.getTargetEdgeCount() == 1);
    fetchedFollower = (Actor) fetchedActor.getTargetEdge(0).getSource();
    assertTrue(fetchedFollower.getName() != null);
    assertTrue(info.follower.getName().equals(fetchedFollower.getName()));

  }

  public void testUpdate() throws IOException {
    GraphInfo info = createGraph();

    String xml = this.serializeGraph(info.actor.getDataGraph());
    log.debug("inserting initial graph:");
    log.debug(xml);
    this.service.commit(info.actor.getDataGraph(), "test1");

    Blog blog = info.follower.createBlog();
    blog.setName("Fiscal Cliff");
    blog.setId(UUID.randomUUID().toString());
    blog.setDescription("A blog about the fiscal \"cliff\" scenario post election");
    blog.addTopic(info.politics);
    log.debug("comitting blog update");
    this.service.commit(info.actor.getDataGraph(), "test2");

    log.debug("fetching follower graph");
    Actor fetchedFollowerRoot = fetchGraph(createGraphQuery(info.follower.getName()));
    xml = this.serializeGraph(fetchedFollowerRoot.getDataGraph());
    log.debug(xml);

    assertTrue(fetchedFollowerRoot.getBlogCount() == 1);
    Blog fetchedBlog = (Blog) fetchedFollowerRoot.get("blog[@name='" + blog.getName() + "']");
    assertTrue(fetchedBlog != null);
    assertTrue(fetchedBlog.getTopicCount() == 1);
    Topic fetchedTopic = fetchedBlog.getTopic(0);
    assertTrue(fetchedTopic.getName() != null);
    assertTrue(fetchedTopic.getName().equals(info.politics.getName()));

    // fetchedFollowerRoot.unsetBlog(); FIXME: isSet="true" in change
    // summary
    fetchedBlog.delete(); // note deletes topics fetched in containment
    // graph with blog
    log.debug("comitting blog remove update");
    this.service.commit(fetchedFollowerRoot.getDataGraph(), "test2");
    log.debug("fetching follower graph again");
    fetchedFollowerRoot = fetchGraph(createGraphQuery(info.follower.getName()));
    xml = this.serializeGraph(fetchedFollowerRoot.getDataGraph());
    log.debug(xml);
    assertTrue(fetchedFollowerRoot.getBlogCount() == 0);

    log.debug("comitting follower graph delete");
    fetchedFollowerRoot.delete();
    this.service.commit(fetchedFollowerRoot.getDataGraph(), "test2");
    log.debug("fetching deleted follower graph");
    fetchedFollowerRoot = findGraph(createGraphQuery(info.follower.getName()));
    assertTrue(fetchedFollowerRoot == null);

    log.debug("fetching actor graph");
    Actor fetchedRoot = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedRoot.getDataGraph());
    log.debug(xml);

    assertTrue(fetchedRoot.getTargetEdgeCount() == 1);
    assertFalse(fetchedRoot.getTargetEdge(0).isSetSource());
    Actor fetchedFollower = (Actor) fetchedRoot.getTargetEdge(0).getSource();
    assertTrue(fetchedFollower == null);

  }

  public void testSliceQueries() throws IOException {
    GraphInfo info = createGraph();

    log.debug("inserting initial graph:");
    this.service.commit(info.actor.getDataGraph(), "test1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    assertTrue(fetchedActor.getTargetEdgeCount() == 1);
    Actor fetchedFollower = (Actor) fetchedActor.getTargetEdge(0).getSource();
    assertTrue(fetchedFollower.getName() != null);
    assertTrue(info.follower.getName().equals(fetchedFollower.getName()));

    assertTrue(fetchedActor.getBlogCount() == 2);
    Blog fetchedBlog = (Blog) fetchedActor.get("blog[@name='" + info.electionBlog.getName() + "']");
    assertTrue(fetchedBlog != null);
    assertTrue(fetchedBlog.getTopicCount() == 1);
    Topic fetchedTopic = fetchedBlog.getTopic(0);
    assertTrue(fetchedTopic.getName() != null);
    assertTrue(fetchedTopic.getName().equals(info.politics.getName()));

    log.debug("fetching blog slice");
    Actor fetchedActorSliceRoot = fetchGraph(createBlogPredicateQuery(info.actor.getName(),
        info.electionBlog.getName()));
    xml = this.serializeGraph(fetchedActorSliceRoot.getDataGraph());
    log.debug(xml);
    assertTrue(fetchedActorSliceRoot.getBlogCount() == 1);
    fetchedBlog = (Blog) fetchedActorSliceRoot.get("blog[@name='" + info.electionBlog.getName()
        + "']");
    assertTrue(fetchedBlog != null);
    assertTrue(fetchedActor.getTargetEdgeCount() == 1);
    fetchedFollower = (Actor) fetchedActor.getTargetEdge(0).getSource();
    assertTrue(fetchedFollower.getName() != null);
    assertTrue(info.follower.getName().equals(fetchedFollower.getName()));

    log.debug("fetching photo slice");
    fetchedActorSliceRoot = fetchGraph(createPhotoPredicateQuery(info.actor.getName(),
        info.photo2.getName()));
    xml = this.serializeGraph(fetchedActorSliceRoot.getDataGraph());
    log.debug(xml);
    assertTrue(fetchedActorSliceRoot.getPhotoCount() == 1);
    Photo fetchedPhoto = (Photo) fetchedActorSliceRoot.get("photo[@name='" + info.photo2.getName()
        + "']");
    assertTrue(fetchedPhoto != null);
    assertTrue(fetchedActor.getTargetEdgeCount() == 1);
    fetchedFollower = (Actor) fetchedActor.getTargetEdge(0).getSource();
    assertTrue(fetchedFollower.getName() != null);
    assertTrue(info.follower.getName().equals(fetchedFollower.getName()));

  }

  public void testTopicInsertAndLink() throws IOException {
    Topic physics = createRootTopic("Physics");
    Topic plasmaPhysics = physics.createChild();
    plasmaPhysics.setName("Plasma Physics");
    plasmaPhysics.setId(UUID.randomUUID().toString());

    Topic ionization = plasmaPhysics.createChild();
    ionization.setName("Plasma Ionization");
    ionization.setId(UUID.randomUUID().toString());

    Topic magnetization = plasmaPhysics.createChild();
    magnetization.setName("Plasma Magnetization");
    magnetization.setId(UUID.randomUUID().toString());

    Topic darkEnergy = physics.createChild();
    darkEnergy.setName("Dark Energy");
    darkEnergy.setId(UUID.randomUUID().toString());

    Topic darkMatter = physics.createChild();
    darkMatter.setName("Dark Matter");
    darkMatter.setId(UUID.randomUUID().toString());

    this.service.commit(physics.getDataGraph(), "test1");

    String name = USERNAME_BASE + String.valueOf(System.currentTimeMillis()) + "_example.com";
    Actor actor = createRootActor(name);
    actor.setName(name);
    actor.setId(UUID.randomUUID().toString());
    actor.setDescription("Guy who likes plasma physics...");

    Blog physicsBlog = actor.createBlog();
    physicsBlog.setName("Thoughts on Plasma Magnetization");
    physicsBlog.setId(UUID.randomUUID().toString());
    physicsBlog.setDescription("Magnetization parameters and temperature...");

    // separate it from its graph so we can
    // add to another graph
    magnetization.detach();

    physicsBlog.addTopic(magnetization);

    this.service.commit(actor.getDataGraph(), "test2");

    Actor fetchedActor = fetchGraph(createGraphQuery(name));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    assertTrue(fetchedActor.getBlogCount() == 1);
    Blog fetchedBlog = (Blog) fetchedActor.get("blog[@name='" + physicsBlog.getName() + "']");
    assertTrue(fetchedBlog != null);
    assertTrue(fetchedBlog.getTopicCount() == 1);
    Topic fetchedTopic = fetchedBlog.getTopic(0);
    assertTrue(fetchedTopic.getName() != null);
    assertTrue(fetchedTopic.getName().equals(magnetization.getName()));

  }

  public void testBlogAdd() throws IOException {

    Topic rocks = createRootTopic("Rocks");

    Topic igneousRocks = rocks.createChild();
    igneousRocks.setName("Igneous Rocks");
    igneousRocks.setId(UUID.randomUUID().toString());
    igneousRocks.setId(UUID.randomUUID().toString());

    Topic metamorphicRocks = rocks.createChild();
    metamorphicRocks.setName("Metamorphic Rocks");
    metamorphicRocks.setId(UUID.randomUUID().toString());

    Topic sedementaryRocks = rocks.createChild();
    sedementaryRocks.setName("Sedementary Rocks");
    sedementaryRocks.setId(UUID.randomUUID().toString());

    // commit some topics we can use
    this.service.commit(rocks.getDataGraph(), "test1");

    String name = USERNAME_BASE + String.valueOf(System.currentTimeMillis()) + "@example.com";
    Actor actor = createRootActor(name);
    actor.setName(name);
    actor.setId(UUID.randomUUID().toString());
    actor.setDescription("Guy who likes rocks...");

    Blog igneousRocksBlog = actor.createBlog();
    igneousRocksBlog.setName("Thoughts on Igneous Rocks");
    igneousRocksBlog.setId(UUID.randomUUID().toString());
    igneousRocksBlog.setDescription("Igneous rocks are cool because...");

    // separate it from its graph so we can
    // add to another graph
    // igneousRocks.detach();

    // igneousRocksBlog.addTopic(igneousRocks);

    // commit the actor and his first blog
    this.service.commit(actor.getDataGraph(), "test2");

    // re-fetch the actor w/o his new blog and see if
    // adding blogs works as expected
    Actor simpleActor = fetchGraph(createSimpleActorQuery(name));
    //
    Blog metamorphicRocksBlog = simpleActor.createBlog();
    metamorphicRocksBlog.setName("Thoughts on Metamorphic Rocks");
    metamorphicRocksBlog.setId(UUID.randomUUID().toString());
    metamorphicRocksBlog.setDescription("Metamorphic rocks are cool because...");
    // metamorphicRocks.detach();
    // metamorphicRocksBlog.addTopic(metamorphicRocks);

    this.service.commit(simpleActor.getDataGraph(), "test2");
    Actor fetchedActor = fetchGraph(createActorBlogGraphQuery(name));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);
    // FIXME: pending graph writer fix
    assertTrue(fetchedActor.getBlogCount() == 2);

    Blog sedementaryRocksBlog = simpleActor.createBlog();
    sedementaryRocksBlog.setName("Thoughts on Sedementary Rocks");
    sedementaryRocksBlog.setId(UUID.randomUUID().toString());
    sedementaryRocksBlog.setDescription("Sedementary rocks are cool because...");
    // sedementaryRocks.detach();
    // sedementaryRocksBlog.addTopic(sedementaryRocks);

    this.service.commit(simpleActor.getDataGraph(), "test2");

    fetchedActor = fetchGraph(createActorBlogGraphQuery(name));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);
    // FIXME: pending graph writer fix
    assertTrue(fetchedActor.getBlogCount() == 3);

  }

  public void testDeletePhotos() throws IOException {
    GraphInfo info = createGraph();

    log.debug("inserting initial graph:");
    this.service.commit(info.actor.getDataGraph(), "test1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    // delete all photos
    for (Photo photo : fetchedActor.getPhoto()) {
      photo.delete();
    }
    this.service.commit(fetchedActor.getDataGraph(), "test1");
    log.debug("fetching deleted photo graph");
    fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);
    assertTrue(fetchedActor.getPhotoCount() == 0);
  }

  public void testAddPhotos() throws IOException {
    GraphInfo info = createGraph();

    log.debug("inserting initial graph:");
    this.service.commit(info.actor.getDataGraph(), "test1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    int added = 10;
    for (int i = 0; i < added; i++) {
      Photo photo = fetchedActor.createPhoto();
      photo.setName("added photo " + i);
      photo.setId(UUID.randomUUID().toString());
      photo.setDescription("a description for added photo " + i);
      photo.setContent(photo.getDescription().getBytes());
    }

    this.service.commit(fetchedActor.getDataGraph(), "test1");
    log.debug("fetching added photo graph");
    fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);
    assertTrue(fetchedActor.getPhotoCount() == 2 + added);
  }

  public void testDeleteBlogs() throws IOException {
    GraphInfo info = createGraph();

    log.debug("inserting initial graph:");
    this.service.commit(info.actor.getDataGraph(), "test1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    // delete all blogs
    for (Blog blog : fetchedActor.getBlog()) {
      blog.delete();
    }
    this.service.commit(fetchedActor.getDataGraph(), "test1");
    log.debug("fetching deleted blog graph");
    fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);
    assertTrue(fetchedActor.getBlogCount() == 0);
  }

  public void testDeleteTopics() throws IOException {
    GraphInfo info = createGraph();

    log.debug("inserting initial graph:");
    this.service.commit(info.actor.getDataGraph(), "test1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    // delete all topics linked to a blog with no children
    for (Blog blog : fetchedActor.getBlog()) {
      for (Topic topic : blog.getTopic()) {
        // assertTrue(topic.getStoryCount() > 0);
        if (topic.getChildCount() == 0)
          if (!fetchedActor.getDataGraph().getChangeSummary().isDeleted(topic))
            topic.delete();
      }
    }
    this.service.commit(fetchedActor.getDataGraph(), "test1");
    log.debug("fetching deleted topic graph");
    fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    for (Blog blog : fetchedActor.getBlog()) {
      for (Topic topic : blog.getTopic()) {
        assertTrue(topic.getChildCount() > 0);
      }
    }
  }

  public void testIncrementBlogLikes() throws IOException {
    if (1 == 1)
      return;
    GraphInfo info = createGraph();

    String xml = this.serializeGraph(info.actor.getDataGraph());
    log.debug("inserting initial graph:");
    log.debug(xml);
    this.service.commit(info.actor.getDataGraph(), "actor1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    assertTrue(fetchedActor.getBlogCount() == 2);
    Blog fetchedBlog = (Blog) fetchedActor.get("blog[@name='" + info.electionBlog.getName() + "']");
    assertTrue(fetchedBlog != null);
    assertTrue(fetchedBlog.getTopicCount() == 1);
    assertTrue(fetchedBlog.getLikes() == 0);

    fetchedBlog.incrementLikes(1);
    this.service.commit(fetchedActor.getDataGraph(), "actor1");
    assertTrue(fetchedBlog.getLikes() == 1);

    Actor fetchedActor2 = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor2.getDataGraph());
    log.debug(xml);

    assertTrue(fetchedActor2.getBlogCount() == 2);
    Blog fetchedBlog2 = (Blog) fetchedActor2.get("blog[@name='" + info.electionBlog.getName()
        + "']");
    assertTrue(fetchedBlog2 != null);
    assertTrue(fetchedBlog2.getTopicCount() == 1);
    assertTrue(fetchedBlog2.getLikes() == 1);

    fetchedBlog2.incrementLikes(4);
    this.service.commit(fetchedActor2.getDataGraph(), "actor2");
    assertTrue(fetchedBlog2.getLikes() == 5); // expect the accumulated value

    fetchedBlog2.unsetLikes();
    this.service.commit(fetchedActor2.getDataGraph(), "actor2");
    assertTrue(fetchedBlog2.getLikes() == 0); // expect zero after unset

    fetchedBlog.incrementLikes(25);
    this.service.commit(fetchedActor.getDataGraph(), "actor1");
    assertTrue("unexpected, " + fetchedBlog.getLikes(), fetchedBlog.getLikes() == 25);

    fetchedBlog2.incrementLikes(10);
    this.service.commit(fetchedActor2.getDataGraph(), "actor2");
    assertTrue(fetchedBlog2.getLikes() == 35); // expect the accumulated value

  }
}
