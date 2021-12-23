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
package test;

import java.io.IOException;
import java.util.UUID;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.cloudgraph.common.CommonTest;
import org.cloudgraph.common.CommonTestSetup;
import org.cloudgraph.mapreduce.GraphWritable;
import org.cloudgraph.mapreduce.GraphXmlMapper;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.cloudgraph.test.socialgraph.story.Blog;
import org.plasma.sdo.helper.PlasmaDataFactory;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.DataGraph;
import commonj.sdo.Type;

public class GraphXmlTest extends CommonTest {
  static final Log log = LogFactory.getLog(GraphXmlTest.class);

  private MapDriver<LongWritable, GraphWritable, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;
  private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

  public static Test suite() {
    return CommonTestSetup.newTestSetup(GraphXmlTest.class);
  }

  public void setUp() {
    TestMapper mapper = new TestMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
  }

  public void testMapper() throws IOException {

    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(Actor.class);
    Actor root = (Actor) dataGraph.createRootObject(rootType);
    root.setName("actor 1");
    root.setId(UUID.randomUUID().toString());
    Blog blog = root.createBlog();
    blog.setName("my blog");

    mapDriver.withInput(new LongWritable(1), new GraphWritable(dataGraph));
    mapDriver.runTest();
  }

  public void testInputFormat() throws IOException {

    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(Actor.class);
    Actor root = (Actor) dataGraph.createRootObject(rootType);
    root.setName("actor 1");
    root.setId(UUID.randomUUID().toString());
    Blog blog = root.createBlog();
    blog.setName("my blog");
    blog.setId(UUID.randomUUID().toString());

    mapDriver.withInput(new LongWritable(1), new GraphWritable(dataGraph));
    mapDriver.runTest();
  }

  public class TestMapper extends GraphXmlMapper<Text, Text> {
    @Override
    public void map(LongWritable k, GraphWritable v, Context context) {
      try {
        log.info(v.toXMLString());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static class TestReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
    }
  }

}
