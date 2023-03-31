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
package org.cloudgraph.rocksdb.scan;

import java.io.IOException;
import java.util.Date;

import junit.framework.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rocksdb.test.DataTypeGraphModelTest;
import org.cloudgraph.test.datatypes.DateTimeNode;
import org.cloudgraph.test.datatypes.Node;
import org.cloudgraph.test.datatypes.query.QDateTimeNode;
import org.plasma.common.test.PlasmaTestSetup;
import org.plasma.query.Expression;
import org.plasma.sdo.helper.PlasmaDataFactory;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.DataGraph;
import commonj.sdo.Type;

/**
 * DateTime SDO datatype specific partial row-key scan operations test.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class DateTimePartialRowKeyScanTest extends DataTypeGraphModelTest {
  private static Log log = LogFactory.getLog(DateTimePartialRowKeyScanTest.class);
  private int WAIT_TIME = 10;
  private String USERNAME = "datetime_test";

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(DateTimePartialRowKeyScanTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testEqual() throws IOException {
    int rootId = Integer.valueOf(String.valueOf(System.nanoTime()).substring(7));

    int id1 = rootId + WAIT_TIME;
    Date now1 = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now1);
    service.commit(root1.getDataGraph(), USERNAME);

    int id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2);
    service.commit(root2.getDataGraph(), USERNAME);

    int id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3);
    service.commit(root3.getDataGraph(), USERNAME);

    // fetch a slice
    Node fetched = this.fetchSingleGraph(rootId, id2, root2.getChild(3).getName(),
        root2.getDateTimeField());
    debugGraph(fetched.getDataGraph());
  }

  /*
   * public void testBetween() throws IOException { int rootId =
   * Integer.valueOf(String.valueOf(System.nanoTime()).substring(7));
   * 
   * int id1 = rootId + WAIT_TIME; Date now1 = new Date(id1); Node root1 =
   * this.createGraph(rootId, id1, now1); service.commit(root1.getDataGraph(),
   * USERNAME);
   * 
   * int id2 = id1 + WAIT_TIME; Date now2 = new Date(id2); Node root2 =
   * this.createGraph(rootId, id2, now2); service.commit(root2.getDataGraph(),
   * USERNAME);
   * 
   * int id3 = id2 + WAIT_TIME; Date now3 = new Date(id3); Node root3 =
   * this.createGraph(rootId, id3, now3); service.commit(root3.getDataGraph(),
   * USERNAME);
   * 
   * Node[] fetched = this.fetchGraphsBetween(rootId, id1, id3,
   * root1.getDateTimeField(), root3.getDateTimeField());
   * assertTrue(fetched.length == 3);
   * 
   * // assertTrue(fetchedProfiles[0].getProfileId() == id1);
   * debugGraph(fetched[0].getDataGraph());
   * debugGraph(fetched[1].getDataGraph());
   * debugGraph(fetched[2].getDataGraph()); }
   * 
   * public void testInclusive() throws IOException { int rootId =
   * Integer.valueOf(String.valueOf(System.nanoTime()).substring(7));
   * 
   * int id1 = rootId + WAIT_TIME; Date now1 = new Date(id1); Node root1 =
   * this.createGraph(rootId, id1, now1); service.commit(root1.getDataGraph(),
   * USERNAME);
   * 
   * int id2 = id1 + WAIT_TIME; Date now2 = new Date(id2); Node root2 =
   * this.createGraph(rootId, id2, now2); service.commit(root2.getDataGraph(),
   * USERNAME);
   * 
   * int id3 = id2 + WAIT_TIME; Date now3 = new Date(id3); Node root3 =
   * this.createGraph(rootId, id3, now3); service.commit(root3.getDataGraph(),
   * USERNAME);
   * 
   * Node[] fetched = this.fetchGraphsInclusive(rootId, id1, id3,
   * root1.getDateTimeField(), root3.getDateTimeField());
   * assertTrue(fetched.length == 3);
   * 
   * debugGraph(fetched[0].getDataGraph());
   * debugGraph(fetched[1].getDataGraph());
   * debugGraph(fetched[2].getDataGraph()); }
   * 
   * public void testExclusive() throws IOException { int rootId =
   * Integer.valueOf(String.valueOf(System.nanoTime()).substring(7));
   * 
   * int id1 = rootId + WAIT_TIME; Date now1 = new Date(id1); Node root1 =
   * this.createGraph(rootId, id1, now1); service.commit(root1.getDataGraph(),
   * USERNAME);
   * 
   * int id2 = id1 + WAIT_TIME; Date now2 = new Date(id2); Node root2 =
   * this.createGraph(rootId, id2, now2); service.commit(root2.getDataGraph(),
   * USERNAME);
   * 
   * int id3 = id2 + WAIT_TIME; Date now3 = new Date(id3); Node root3 =
   * this.createGraph(rootId, id3, now3); service.commit(root3.getDataGraph(),
   * USERNAME);
   * 
   * Node[] fetched = this.fetchGraphsExclusive(id1, id3,
   * root1.getDateTimeField(), root3.getDateTimeField());
   * assertTrue(fetched.length == 1);
   * 
   * debugGraph(fetched[0].getDataGraph()); }
   */
  protected Node fetchSingleGraph(long rootId, long id, String name, Object date) {
    QDateTimeNode root = createSelect(name);

    root.where(root.rootId().eq(rootId).and(root.dateTimeField().eq(date)));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Node) result[0].getRootObject();
  }

  protected Node[] fetchGraphsBetween(long rootId, long min, long max, Object minDate,
      Object maxDate) {
    QDateTimeNode root = createSelect();
    root.where(root.rootId().eq(rootId).and(root.dateTimeField().between(minDate, maxDate)));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  protected Node[] fetchGraphsInclusive(long rootId, long min, long max, Object minDate,
      Object maxDate) {
    QDateTimeNode root = createSelect();
    root.where(root.rootId().eq(rootId)
        .and(root.dateTimeField().ge(minDate).and(root.dateTimeField().le(maxDate))));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  protected Node[] fetchGraphsExclusive(long min, long max, Object minDate, Object maxDate) {
    QDateTimeNode root = createSelect();
    root.where(root.dateTimeField().gt(minDate).and(root.dateTimeField().lt(maxDate)));
    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  private QDateTimeNode createSelect(String name) {
    QDateTimeNode root = QDateTimeNode.newQuery();
    Expression predicate = root.name().eq(name);
    root.select(root.wildcard());
    root.select(root.child(predicate).wildcard());
    root.select(root.child(predicate).child().wildcard());
    root.select(root.child(predicate).child().child().wildcard());
    root.select(root.child(predicate).child().child().child().wildcard());
    return root;
  }

  private QDateTimeNode createSelect() {
    QDateTimeNode root = QDateTimeNode.newQuery();
    root.select(root.wildcard());
    root.select(root.child().wildcard());
    root.select(root.child().child().wildcard());
    root.select(root.child().child().child().wildcard());
    root.select(root.child().child().child().child().wildcard());
    return root;
  }

  protected DateTimeNode createGraph(int rootId, int id, Date now) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(DateTimeNode.class);
    DateTimeNode root = (DateTimeNode) dataGraph.createRootObject(rootType);
    fillNode(root, rootId, id, now, "datetime", 0, 0);
    fillGraph(root, id, now, "datetime");
    return root;
  }

}
