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
package org.cloudgraph.hbase.test;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.cloudgraph.test.socialgraph.actor.Friendship;
import org.cloudgraph.test.socialgraph.actor.Photo;
import org.cloudgraph.test.socialgraph.actor.Topic;
import org.cloudgraph.test.socialgraph.actor.query.QActor;
import org.cloudgraph.test.socialgraph.actor.query.QPhoto;
import org.cloudgraph.test.socialgraph.story.Blog;
import org.cloudgraph.test.socialgraph.story.query.QBlog;
import org.plasma.query.Expression;
import org.plasma.sdo.helper.PlasmaDataFactory;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.DataGraph;
import commonj.sdo.Type;

/**
 * @author Scott Cinnamond
 * @since 0.5
 */
public abstract class SocialGraphModelTest extends HBaseTestCase {
  private static Log log = LogFactory.getLog(SocialGraphModelTest.class);

  protected long WAIT_TIME = 1000;
  protected String USERNAME_BASE = "social";

  protected class GraphInfo {
    public Actor actor;
    public Friendship friendship;
    public Actor follower;
    public Blog weatherBlog;
    public Blog electionBlog;
    public Topic weather;
    public Topic atlanticWeather;
    public Topic atlanticStorms;
    public Topic politics;
    public Photo photo;
    public Photo photo2;
  }

  protected GraphInfo createGraph() throws IOException {
    return this.createGraph(USERNAME_BASE);
  }

  protected GraphInfo createGraph(String usernamePrefix) throws IOException {
    GraphInfo info = new GraphInfo();

    String name = usernamePrefix + String.valueOf(System.nanoTime()).substring(10) + "test.com";
    info.actor = createRootActor(name);
    info.actor.setName(name);
    info.actor.setId(UUID.randomUUID().toString());
    info.actor.setDescription("I'm a guy who likes storms...");

    String followerName = "fol" + String.valueOf(System.nanoTime()).substring(10) + "test.com";

    info.friendship = (Friendship) info.actor.createTargetEdge(Friendship.class);

    info.follower = (Actor) info.friendship.createSource(Actor.class);
    info.follower.setName(followerName);
    info.follower.setId(UUID.randomUUID().toString());
    info.follower.setDescription("I'm a follower of the other guy...");

    info.weatherBlog = info.actor.createBlog();
    info.weatherBlog.setName("Hurricane Sandy");
    info.weatherBlog.setId(UUID.randomUUID().toString());
    info.weatherBlog.setDescription("The recent east coast hurricane...");

    info.weather = info.weatherBlog.createTopic();
    info.weather.setName("Weather");
    info.weather.setId(UUID.randomUUID().toString());
    info.weather.setDescription("a topic related to weather");

    info.atlanticWeather = info.weather.createChild();
    info.atlanticWeather.setName("Atlantic Weather");
    info.atlanticWeather.setId(UUID.randomUUID().toString());
    info.atlanticWeather.setDescription("a topic related to weather specific to the Atlantic");

    info.atlanticStorms = info.atlanticWeather.createChild();
    info.atlanticStorms.setName("Atlantic Storms");
    info.atlanticStorms.setId(UUID.randomUUID().toString());
    info.atlanticStorms
        .setDescription("a topic related to stormy weather specific to the Atlantic");

    info.weatherBlog.addTopic(info.atlanticStorms); // link to child topic

    info.electionBlog = info.actor.createBlog();
    info.electionBlog.setName("2012 Presidential Election");
    info.electionBlog.setId(UUID.randomUUID().toString());
    info.electionBlog.setDescription("Thoughts on the 2012 election...");

    info.politics = info.electionBlog.createTopic();
    info.politics.setName("Politics");
    info.politics.setId(UUID.randomUUID().toString());
    info.politics.setDescription("a topic related to politics");

    info.weatherBlog.addTopic(info.politics);

    info.photo = info.actor.createPhoto();
    info.photo.setName("sandy1");
    info.photo.setId(UUID.randomUUID().toString());
    info.photo.setDescription("a photo of hurricane Sandy");
    info.photo.setContent(info.photo.getDescription().getBytes());

    info.photo2 = info.actor.createPhoto();
    info.photo2.setName("sandy2");
    info.photo2.setId(UUID.randomUUID().toString());
    info.photo2.setDescription("another photo of hurricane Sandy");
    info.photo2.setContent(info.photo2.getDescription().getBytes());

    return info;
  }

  protected GraphInfo createSimpleGraph() throws IOException {
    GraphInfo info = new GraphInfo();

    String name = USERNAME_BASE + String.valueOf(System.nanoTime()).substring(10) + "_test.com";
    info.actor = createRootActor(name);
    info.actor.setName(name);
    info.actor.setId(UUID.randomUUID().toString());
    info.actor.setDescription("I'm a guy who likes storms...");

    info.weatherBlog = info.actor.createBlog();
    info.weatherBlog.setName("Hurricane Sandy");
    info.weatherBlog.setId(UUID.randomUUID().toString());
    info.weatherBlog.setDescription("The recent east coast hurricane...");

    info.weather = info.weatherBlog.createTopic();
    info.weather.setName("Weather");
    info.weather.setId(UUID.randomUUID().toString());
    info.weather.setDescription("a topic related to weather");

    info.atlanticWeather = info.weather.createChild();
    info.atlanticWeather.setName("Atlantic Weather");
    info.atlanticWeather.setId(UUID.randomUUID().toString());
    info.atlanticWeather.setDescription("a topic related to weather specific to the Atlantic");

    info.atlanticStorms = info.atlanticWeather.createChild();
    info.atlanticStorms.setName("Atlantic Storms");
    info.atlanticStorms.setId(UUID.randomUUID().toString());
    info.atlanticStorms
        .setDescription("a topic related to stormy weather specific to the Atlantic");

    info.weatherBlog.addTopic(info.atlanticStorms); // link to child topic

    info.electionBlog = info.actor.createBlog();
    info.electionBlog.setName("2012 Presidential Election");
    info.electionBlog.setId(UUID.randomUUID().toString());
    info.electionBlog.setDescription("Thoughts on the 2012 election...");

    info.politics = info.electionBlog.createTopic();
    info.politics.setName("Politics");
    info.politics.setId(UUID.randomUUID().toString());
    info.politics.setDescription("a topic related to politics");

    // Sandy changed the election so...
    // now politics topic has 2 blog parents
    info.weatherBlog.addTopic(info.politics);

    info.photo = info.actor.createPhoto();
    info.photo.setName("sandy1");
    info.photo.setId(UUID.randomUUID().toString());
    info.photo.setDescription("a photo of hurricane Sandy");
    info.photo.setContent(info.photo.getDescription().getBytes());

    info.photo2 = info.actor.createPhoto();
    info.photo2.setName("sandy2");
    info.photo2.setId(UUID.randomUUID().toString());
    info.photo2.setDescription("another photo of hurricane Sandy");
    info.photo2.setContent(info.photo.getDescription().getBytes());

    return info;
  }

  protected Actor createRootActor(String name) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(Actor.class);
    Actor root = (Actor) dataGraph.createRootObject(rootType);
    root.setName(name);
    root.setId(UUID.randomUUID().toString());

    return root;
  }

  protected Topic createRootTopic(String name) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(Topic.class);
    Topic root = (Topic) dataGraph.createRootObject(rootType);
    root.setName(name);
    root.setId(UUID.randomUUID().toString());

    return root;
  }

  protected Actor findGraph(QActor query) {

    this.marshal(query.getModel(), "social");

    DataGraph[] result = service.find(query);
    assertTrue(result != null);
    if (result != null && result.length > 0) {
      assertTrue(result.length == 1);
      return (Actor) result[0].getRootObject();
    }
    return null;
  }

  protected Actor fetchGraph(QActor query) {

    this.marshal(query.getModel(), "social");

    DataGraph[] result = service.find(query);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Actor) result[0].getRootObject();
  }

  protected Actor[] fetchGraphs(QActor query) {

    this.marshal(query.getModel(), "social");

    DataGraph[] results = service.find(query);
    assertTrue(results != null);

    Actor[] result = new Actor[results.length];
    for (int i = 0; i < results.length; i++)
      result[i] = (Actor) results[i].getRootObject();
    return result;
  }

  protected QActor createSimpleActorQuery(String name) {
    QActor root = QActor.newQuery();
    root.select(root.wildcard());

    root.where(root.name().eq(name));

    return root;
  }

  protected QActor createActorBlogGraphQuery(String name) {
    QActor root = QActor.newQuery();
    root.select(root.wildcard()).select(root.blog().wildcard())
        .select(root.blog().topic().wildcard());

    root.where(root.name().eq(name));

    return root;
  }

  protected QActor createGraphQuery(String name) {
    QActor root = QActor.newQuery();
    root.select(root.wildcard()).select(root.sourceEdge().wildcard())
        .select(root.sourceEdge().source().wildcard())
        .select(root.sourceEdge().target().wildcard()).select(root.targetEdge().wildcard())
        .select(root.targetEdge().source().wildcard())
        .select(root.targetEdge().target().wildcard()).select(root.blog().wildcard())
        .select(root.blog().topic().wildcard()).select(root.blog().topic().child().wildcard())
        .select(root.blog().topic().child().child().wildcard())
        .select(root.blog().topic().parent().wildcard())
        .select(root.blog().topic().parent().parent().wildcard()).select(root.photo().wildcard());

    if (!name.contains("*"))
      root.where(root.name().eq(name));
    else
      root.where(root.name().like(name));
    return root;
  }

  protected QActor createTopicScanQuery(String name, Topic topic) {
    QActor root = QActor.newQuery();
    root.select(root.wildcard()).select(root.sourceEdge().wildcard())
        .select(root.sourceEdge().source().wildcard())
        .select(root.sourceEdge().target().wildcard()).select(root.targetEdge().wildcard())
        .select(root.targetEdge().source().wildcard())
        .select(root.targetEdge().target().wildcard()).select(root.blog().wildcard())
        .select(root.photo().wildcard());

    root.where(root.name().eq(name).and(root.blog().topic().name().eq(topic.getName())));

    return root;
  }

  protected QActor createTopicScanQuery(String name1, String name2, String name3, Topic topic1,
      Topic topic2) {
    QActor root = QActor.newQuery();
    root.select(root.wildcard()).select(root.sourceEdge().wildcard())
        .select(root.sourceEdge().source().wildcard())
        .select(root.sourceEdge().target().wildcard()).select(root.targetEdge().wildcard())
        .select(root.targetEdge().source().wildcard())
        .select(root.targetEdge().target().wildcard()).select(root.blog().wildcard())
        .select(root.photo().wildcard());

    root.where(root
        .name()
        .like(name1.substring(0, 10) + "*")
        .or(root.name().eq(name2))
        .or(root.name().like(name3.substring(0, 10) + "*"))
        .and(
            root.blog().topic().name().eq(topic1.getName())
                .and(root.blog().topic().name().eq(topic2.getName()))));

    return root;
  }

  protected QActor createTopicWildcardScanQuery(String name) {
    QActor root = QActor.newQuery();
    root.select(root.wildcard()).select(root.sourceEdge().wildcard())
        .select(root.sourceEdge().source().wildcard())
        .select(root.sourceEdge().target().wildcard()).select(root.targetEdge().wildcard())
        .select(root.targetEdge().source().wildcard())
        .select(root.targetEdge().target().wildcard()).select(root.blog().wildcard())
        .select(root.blog().topic().wildcard()).select(root.blog().topic().child().wildcard())
        .select(root.blog().topic().child().child().wildcard())
        .select(root.blog().topic().parent().wildcard())
        .select(root.blog().topic().parent().parent().wildcard()).select(root.photo().wildcard());

    root.where(root.name().eq(name)
        .and(root.blog().topic().name().like("*ics").and(root.blog().topic().name().like("*lit*"))));

    return root;
  }

  protected QActor createBlogPredicateQuery(String actorName, String blogName) {
    QActor actor = QActor.newQuery();
    Expression blogPredicate = QBlog.newQuery().name().eq(blogName);

    actor.select(actor.wildcard()).select(actor.sourceEdge().wildcard())
        .select(actor.sourceEdge().source().wildcard())
        .select(actor.sourceEdge().target().wildcard()).select(actor.targetEdge().wildcard())
        .select(actor.targetEdge().source().wildcard())
        .select(actor.targetEdge().target().wildcard())
        .select(actor.blog(blogPredicate).wildcard())
        .select(actor.blog(blogPredicate).topic().wildcard())
        .select(actor.blog(blogPredicate).topic().child().wildcard())
        .select(actor.blog(blogPredicate).topic().child().child().wildcard())
        .select(actor.photo().wildcard());

    actor.where(actor.name().eq(actorName));

    return actor;
  }

  protected QActor createPhotoPredicateQuery(String actorName, String photoName) {
    QActor actor = QActor.newQuery();
    Expression photoPredicate = QPhoto.newQuery().name().eq(photoName);

    actor.select(actor.wildcard()).select(actor.sourceEdge().wildcard())
        .select(actor.sourceEdge().source().wildcard())
        .select(actor.sourceEdge().target().wildcard()).select(actor.targetEdge().wildcard())
        .select(actor.targetEdge().source().wildcard())
        .select(actor.targetEdge().target().wildcard()).select(actor.blog().wildcard())
        .select(actor.blog().topic().wildcard()).select(actor.blog().topic().child().wildcard())
        .select(actor.blog().topic().child().child().wildcard())
        .select(actor.photo(photoPredicate).wildcard());

    actor.where(actor.name().eq(actorName));

    return actor;
  }

  protected QActor createFollowerGraphQuery(String name) {
    QActor actor = QActor.newQuery();
    actor.select(actor.wildcard()).select(actor.sourceEdge().wildcard())
        .select(actor.sourceEdge().source().wildcard())
        .select(actor.sourceEdge().target().wildcard()).select(actor.targetEdge().wildcard())
        .select(actor.targetEdge().source().wildcard())
        .select(actor.targetEdge().target().wildcard());
    // .select(actor.following().wildcard())
    // .select(actor.following().blog().wildcard())
    // .select(actor.following().blog().topic().wildcard())
    // .select(actor.following().photo().wildcard());

    actor.where(actor.name().eq(name));

    return actor;
  }

}