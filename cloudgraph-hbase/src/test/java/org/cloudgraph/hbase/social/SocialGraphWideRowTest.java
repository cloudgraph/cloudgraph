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
package org.cloudgraph.hbase.social;

import java.io.IOException;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.test.SocialGraphModelTest;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.cloudgraph.test.socialgraph.actor.Photo;
import org.cloudgraph.test.socialgraph.actor.Topic;
import org.cloudgraph.test.socialgraph.story.Blog;
import org.plasma.common.test.PlasmaTestSetup;

/**
 * Tests standard operations on a graph, where the graph is stored as a single
 * "wide" row as opposed to being distributed across HBase tables. See the
 * associated *cloudgraph-config.xml which maps only the social graph
 * {@link Actor} root to a table, the remaining graph modes being "unbound" and
 * therefore part of the wide row.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class SocialGraphWideRowTest extends SocialGraphModelTest {
  private static Log log = LogFactory.getLog(SocialGraphWideRowTest.class);
  private long WAIT_TIME = 1000;
  private String USERNAME_BASE = "social";

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(SocialGraphWideRowTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testInsert() throws IOException {
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

    log.debug("fetching follower");
    Actor fetchedFollowerRoot = fetchGraph(createFollowerGraphQuery(info.follower.getName()));
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

  public void testUpdateDataProperties() throws IOException {
    GraphInfo info = createGraph();

    log.debug("inserting initial graph:");
    this.service.commit(info.actor.getDataGraph(), "test1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    Blog blog = (Blog) fetchedActor.get("blog[name='Hurricane Sandy']");
    assertTrue(blog != null);
    blog.setDescription("updated description for this blog");

    int i = 1;
    for (Photo photo : fetchedActor.getPhoto()) {
      photo.setContent("updated photo content".getBytes());
      photo.setDescription(photo.getDescription() + " - updated photo description "
          + String.valueOf(i));
      i++;
    }

    this.service.commit(fetchedActor.getDataGraph(), "test1");
    log.debug("fetching deleted photo graph");
    fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);
    Blog updatedBlog = (Blog) fetchedActor.get("blog[name='Hurricane Sandy']");
    assertTrue(updatedBlog != null);
    assertTrue(blog.getDescription().contains("updated"));

    assertTrue(fetchedActor.getPhotoCount() == 2);
    for (Photo photo : fetchedActor.getPhoto()) {
      assertTrue((new String(photo.getContent()).contains("updated")));
      assertTrue(photo.getDescription().contains("updated"));
    }
  }

  public void testUpdateAddChildTopic() throws IOException {
    GraphInfo info = createGraph();

    log.debug("inserting initial graph:");
    this.service.commit(info.actor.getDataGraph(), "test1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    Topic topic = (Topic) fetchedActor.get("blog[name='Hurricane Sandy']/topic[name='Politics']");
    assertTrue(topic != null);
    assertTrue(topic.getChildCount() == 0);

    Topic child1 = topic.createChild();
    child1.setName("Chris Christie");
    child1.setDescription("The new Jersey Governor as relates to Hurricane Sandy");

    this.service.commit(fetchedActor.getDataGraph(), "test1");
    log.debug("fetching updated graph");
    fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);
    topic = (Topic) fetchedActor.get("blog[name='Hurricane Sandy']/topic[name='Politics']");
    assertTrue(topic != null);
    assertTrue(topic.getChildCount() == 1);
    assertTrue(topic.getChild(0).getName().equals("Chris Christie"));
  }

  public void testUpdateAddChildTopicHierarchy() throws IOException {
    GraphInfo info = createGraph();

    log.debug("inserting initial graph:");
    this.service.commit(info.actor.getDataGraph(), "test1");

    log.debug("fetching initial graph");
    Actor fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    String xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);

    Topic topic = (Topic) fetchedActor.get("blog[name='Hurricane Sandy']/topic[name='Politics']");
    assertTrue(topic != null);
    assertTrue(topic.getChildCount() == 0);

    Topic child1 = topic.createChild();
    child1.setName("Chris Christie");
    child1.setDescription("The new Jersey Governor as relates to Hurricane Sandy");

    Topic grandChild1 = child1.createChild();
    grandChild1.setName("Bridge Gate");
    grandChild1.setDescription("The new Jersey Governor and his 'Bridge Gate' scandal");
    Topic grandChild2 = child1.createChild();
    grandChild2.setName("Beach Gate");
    grandChild2
        .setDescription("The new Jersey Governor and his 'Beach Gate' scandal where he forbid all NJ citizens from the beach, then went there w/his family");

    this.service.commit(fetchedActor.getDataGraph(), "test1");
    log.debug("fetching updated graph");
    fetchedActor = fetchGraph(createGraphQuery(info.actor.getName()));
    xml = this.serializeGraph(fetchedActor.getDataGraph());
    log.debug(xml);
    topic = (Topic) fetchedActor.get("blog[name='Hurricane Sandy']/topic[name='Politics']");
    assertTrue(topic != null);
    assertTrue(topic.getChildCount() == 1);
    Topic christie = topic.getChild(0);
    assertTrue(christie.getName().equals("Chris Christie"));
    assertTrue(christie.getChildCount() == 2);
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

}
