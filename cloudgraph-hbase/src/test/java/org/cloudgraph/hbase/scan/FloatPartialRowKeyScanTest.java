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
package org.cloudgraph.hbase.scan;

import java.io.IOException;
import java.util.Date;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.test.DataTypeGraphModelTest;
import org.cloudgraph.test.datatypes.FloatNode;
import org.cloudgraph.test.datatypes.Node;
import org.cloudgraph.test.datatypes.query.QFloatNode;
import org.plasma.common.test.PlasmaTestSetup;
import org.plasma.query.Expression;
import org.plasma.sdo.helper.PlasmaDataFactory;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.DataGraph;
import commonj.sdo.Type;

/**
 * Float SDO datatype specific partial row-key scan operations test.
 */
public class FloatPartialRowKeyScanTest extends DataTypeGraphModelTest {
  private static Log log = LogFactory.getLog(FloatPartialRowKeyScanTest.class);
  private long INCREMENT = 1500;
  private long WAIT_TIME = 10;
  private String USERNAME = "float_test";

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(FloatPartialRowKeyScanTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testEqual() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + INCREMENT;
    Date now1 = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now1, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + INCREMENT;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + INCREMENT;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    // fetch a slice
    String sliceName = root1.getChild(3).getName();
    Node fetched = this.fetchSingleGraph(rootId, root1.getFloatField(), sliceName);
    debugGraph(fetched.getDataGraph());
    assertTrue(fetched.getChildCount() == 1); // expect single slice
    assertTrue(fetched.getRootId() == rootId);
    String name = fetched.getString("child[@name='" + sliceName + "']/@name");
    assertTrue(name.equals(sliceName));
  }

  public void testBetween() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + INCREMENT;
    Date now1 = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now1, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + INCREMENT;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + INCREMENT;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this.fetchGraphsBetween(rootId, root1.getFloatField(), root3.getFloatField());
    assertTrue(fetched.length == 3);

    debugGraph(fetched[0].getDataGraph());
    debugGraph(fetched[1].getDataGraph());
    debugGraph(fetched[2].getDataGraph());

  }

  public void testInclusive() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + INCREMENT;
    Date now1 = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now1, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + INCREMENT;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + INCREMENT;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this
        .fetchGraphsInclusive(rootId, root1.getFloatField(), root3.getFloatField());
    assertTrue(fetched.length == 3);
    debugGraph(fetched[0].getDataGraph());
    debugGraph(fetched[1].getDataGraph());
    debugGraph(fetched[2].getDataGraph());
  }

  public void testExclusive() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + INCREMENT;
    Date now1 = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now1, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + INCREMENT;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + INCREMENT;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this
        .fetchGraphsExclusive(rootId, root1.getFloatField(), root3.getFloatField());
    assertTrue(fetched.length == 1);
    debugGraph(fetched[0].getDataGraph());
  }

  protected Node fetchSingleGraph(long rootId, Float id, String name) {
    QFloatNode root = createSelect(name);
    root.where(root.rootId().eq(rootId).and(root.floatField().eq(id)));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Node) result[0].getRootObject();
  }

  protected Node[] fetchGraphsBetween(long rootId, Float min, Float max) {
    QFloatNode root = createSelect();
    root.where(root.rootId().eq(rootId).and(root.floatField().between(min, max)));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  protected Node[] fetchGraphsInclusive(long rootId, Float min, Float max) {
    QFloatNode root = createSelect();
    root.where(root.rootId().eq(rootId)
        .and(root.floatField().ge(min).and(root.floatField().le(max))));
    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  protected Node[] fetchGraphsExclusive(long rootId, Float min, Float max) {
    QFloatNode root = createSelect();
    root.where(root.rootId().eq(rootId)
        .and(root.floatField().gt(min).and(root.floatField().lt(max))));
    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  private QFloatNode createSelect(String name) {
    QFloatNode root = QFloatNode.newQuery();
    Expression predicate = root.name().eq(name);
    root.select(root.wildcard());
    root.select(root.child(predicate).wildcard());
    root.select(root.child(predicate).child().wildcard());
    root.select(root.child(predicate).child().child().wildcard());
    root.select(root.child(predicate).child().child().child().wildcard());
    return root;
  }

  private QFloatNode createSelect() {
    QFloatNode root = QFloatNode.newQuery();
    root.select(root.wildcard());
    root.select(root.child().wildcard());
    root.select(root.child().child().wildcard());
    root.select(root.child().child().child().wildcard());
    root.select(root.child().child().child().child().wildcard());
    return root;
  }

  protected FloatNode createGraph(long rootId, long id, Date now, String namePrefix) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(FloatNode.class);
    FloatNode root = (FloatNode) dataGraph.createRootObject(rootType);
    fillNode(root, rootId, id, now, namePrefix, 0, 0);
    fillGraph(root, id, now, namePrefix);
    return root;
  }

}
