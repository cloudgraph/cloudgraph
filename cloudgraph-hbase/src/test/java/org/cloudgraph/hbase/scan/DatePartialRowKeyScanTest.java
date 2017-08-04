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
import org.cloudgraph.test.datatypes.DateNode;
import org.cloudgraph.test.datatypes.Node;
import org.cloudgraph.test.datatypes.query.QDateNode;
import org.plasma.common.test.PlasmaTestSetup;
import org.plasma.query.Expression;
import org.plasma.sdo.helper.PlasmaDataFactory;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.DataGraph;
import commonj.sdo.Type;

/**
 * Date SDO datatype specific partial row-key scan operations test.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class DatePartialRowKeyScanTest extends DataTypeGraphModelTest {
  private static Log log = LogFactory.getLog(DatePartialRowKeyScanTest.class);
  private long WAIT_TIME = 1050;
  private String USERNAME = "date_test";

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(DatePartialRowKeyScanTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testEqual() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now = new Date(id1);
    Node root = this.createGraph(rootId, id1, now);
    service.commit(root.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2);
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3);
    service.commit(root3.getDataGraph(), USERNAME);

    // fetch a slice
    Node fetched = this.fetchSingleGraph(rootId, root.getChild(3).getName(), root.getDateField());
    debugGraph(fetched.getDataGraph());
    // FIXME: rootid is inherited and not being returned !!
    // assertTrue(fetched.getRootId() == id);
  }

  public void testBetween() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now);
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2);
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3);
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this.fetchGraphsBetween(rootId, id1, id3, root1.getDateField(),
        root3.getDateField());
    assertTrue(fetched.length == 3);

    // assertTrue(fetchedProfiles[0].getProfileId() == id1);
    logGraph(fetched[0].getDataGraph());

    // assertTrue(fetchedProfiles[1].getProfileId() == id2);
    logGraph(fetched[1].getDataGraph());

    // assertTrue(fetchedProfiles[2].getProfileId() == id3);
    logGraph(fetched[2].getDataGraph());

  }

  public void testInclusive() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now);
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2);
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3);
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this.fetchGraphsInclusive(rootId, id1, id3, root1.getDateField(),
        root3.getDateField());
    assertTrue(fetched.length == 3);

    // assertTrue(fetchedProfiles[0].getProfileId() == id1);
    logGraph(fetched[0].getDataGraph());
    logGraph(fetched[1].getDataGraph());
    logGraph(fetched[2].getDataGraph());
  }

  public void testExclusive() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now);
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2);
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3);
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this.fetchGraphsExclusive(rootId, id1, id3, root1.getDateField(),
        root3.getDateField());
    assertTrue(fetched.length == 1);

    logGraph(fetched[0].getDataGraph());
  }

  protected Node fetchSingleGraph(long id, String name, Object date) {
    QDateNode root = createSelect(name);

    root.where(root.rootId().eq(id).and(root.dateField().eq(date)));
    // root.where(root.dateField().eq(date));
    this.marshal(root.getModel(), id);

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Node) result[0].getRootObject();
  }

  protected Node[] fetchGraphsBetween(long rootId, long min, long max, Object minDate,
      Object maxDate) {
    QDateNode root = createSelect();
    root.where(root.rootId().eq(rootId).and(root.dateField().between(minDate, maxDate)));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  protected Node[] fetchGraphsInclusive(long rootId, long min, long max, Object minDate,
      Object maxDate) {
    QDateNode root = createSelect();
    root.where(root.rootId().eq(rootId)
        .and(root.dateField().ge(minDate).and(root.dateField().le(maxDate))));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  protected Node[] fetchGraphsExclusive(long rootId, long min, long max, Object minDate,
      Object maxDate) {
    QDateNode root = createSelect();
    root.where(root.rootId().eq(rootId)
        .and(root.dateField().gt(minDate).and(root.dateField().lt(maxDate))));
    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  private QDateNode createSelect(String name) {
    QDateNode root = QDateNode.newQuery();
    Expression predicate = root.name().eq(name);
    root.select(root.wildcard());
    root.select(root.child(predicate).wildcard());
    root.select(root.child(predicate).child().wildcard());
    root.select(root.child(predicate).child().child().wildcard());
    root.select(root.child(predicate).child().child().child().wildcard());
    return root;
  }

  private QDateNode createSelect() {
    QDateNode root = QDateNode.newQuery();
    root.select(root.wildcard());
    root.select(root.child().wildcard());
    root.select(root.child().child().wildcard());
    root.select(root.child().child().child().wildcard());
    root.select(root.child().child().child().child().wildcard());
    return root;
  }

  protected DateNode createGraph(long rootId, long id, Date now) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(DateNode.class);
    DateNode root = (DateNode) dataGraph.createRootObject(rootType);
    fillNode(root, rootId, id, now, "date", 0, 0);
    fillGraph(root, id, now, "date");
    return root;
  }

}
