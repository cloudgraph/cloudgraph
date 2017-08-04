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
import org.cloudgraph.test.datatypes.Node;
import org.cloudgraph.test.datatypes.query.QStringNode;
import org.plasma.common.test.PlasmaTestSetup;

import commonj.sdo.DataGraph;

/**
 * String SDO datatype specific partial row-key scan operations test.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */

public class StringPartialRowKeyScanTest extends StringScanTest {
  private static Log log = LogFactory.getLog(StringPartialRowKeyScanTest.class);

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(StringPartialRowKeyScanTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testEqual() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now1 = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now1, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    // fetch
    Node fetched = this.fetchSingleGraph(root2.getRootId(), root2.getStringField());
    String xml = serializeGraph(fetched.getDataGraph());
    log.debug("GRAPH: " + xml);
    assertTrue(fetched.getRootId() == rootId);
  }

  public void testEqual2() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now1 = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now1, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "AAA 222");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "AAA 333");
    service.commit(root3.getDataGraph(), USERNAME);

    // fetch
    Node fetched = this.fetchSingleGraph(root1.getRootId(), "AAA_0_0"); // note:
    // root
    // node
    // gets
    // '_0_0'
    // suffix
    // expecting only first node NOT 'AAA 222_0_0' or 'AAA 333_0_0'
    String xml = serializeGraph(fetched.getDataGraph());
    log.debug("GRAPH: " + xml);
    assertTrue(fetched.getRootId() == rootId);
  }

  public void testPartialKeyWildcard() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now1 = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now1, "AAA 111");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "AzB 112");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "A_zC 113");
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this.fetchGraphsLike(rootId, "A*");
    assertTrue("expected 3 results not " + fetched.length, fetched.length == 3);
    // debugGraph(fetched[0].getDataGraph());
    // debugGraph(fetched[1].getDataGraph());
    // debugGraph(fetched[2].getDataGraph());
  }

  public void testBetween() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this
        .fetchGraphsBetween(rootId, root1.getStringField(), root3.getStringField());
    assertTrue("expected 3 results not " + fetched.length, fetched.length == 3);
    debugGraph(fetched[0].getDataGraph());
    debugGraph(fetched[1].getDataGraph());
    debugGraph(fetched[2].getDataGraph());

  }

  public void testInclusive() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this.fetchGraphsInclusive(rootId, root1.getStringField(),
        root3.getStringField());
    assertTrue(fetched.length == 3);
    debugGraph(fetched[0].getDataGraph());
    debugGraph(fetched[1].getDataGraph());
    debugGraph(fetched[2].getDataGraph());
  }

  public void testExclusive() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    Node[] fetched = this.fetchGraphsExclusive(rootId, root1.getStringField(),
        root3.getStringField());
    assertTrue(fetched.length == 1);

    debugGraph(fetched[0].getDataGraph());
  }

  public void testRange() throws IOException {
    long rootId = System.currentTimeMillis();

    long id1 = rootId + WAIT_TIME;
    Date now = new Date(id1);
    Node root1 = this.createGraph(rootId, id1, now, "AAA");
    service.commit(root1.getDataGraph(), USERNAME);

    long id2 = id1 + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(root2.getDataGraph(), USERNAME);

    long id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    long id4 = id3 + WAIT_TIME;
    Date now4 = new Date(id4);
    Node root4 = this.createGraph(rootId, id4, now4, "DDD");
    service.commit(root4.getDataGraph(), USERNAME);

    long id5 = id4 + WAIT_TIME;
    Date now5 = new Date(id5);
    Node root5 = this.createGraph(rootId, id5, now5, "EEE");
    service.commit(root5.getDataGraph(), USERNAME);

    QStringNode query = createSelect();
    query.where(query.rootId().eq(rootId).and(query.stringField().ge(root1.getStringField()))
        .and(query.stringField().le(root5.getStringField())));
    query.orderBy(query.stringField());

    DataGraph[] result = service.find(query);
    assertTrue(result != null);
    assertTrue(result.length == 5);
    debugGraph(result[0]);
    assertTrue("AAA_0_0".equals(((Node) result[0].getRootObject()).getStringField()));
    assertTrue("EEE_0_0".equals(((Node) result[4].getRootObject()).getStringField()));

    query.setStartRange(1);
    query.setEndRange(3);
    result = service.find(query);
    assertTrue(result != null);
    assertTrue(result.length == 3);
    assertTrue("AAA_0_0".equals(((Node) result[0].getRootObject()).getStringField()));
    assertTrue("CCC_0_0".equals(((Node) result[2].getRootObject()).getStringField()));

    query.setStartRange(3);
    query.setEndRange(4);
    result = service.find(query);
    assertTrue(result != null);
    assertTrue(result.length == 2);
    assertTrue("CCC_0_0".equals(((Node) result[0].getRootObject()).getStringField()));
    assertTrue("DDD_0_0".equals(((Node) result[1].getRootObject()).getStringField()));
  }

  protected Node fetchSingleGraph(long id, String name) {
    QStringNode root = createSelect();

    root.where(root.rootId().eq(id).and(root.stringField().eq(name)));

    this.marshal(root.getModel(), id);

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    // FIXME: failing
    assertTrue("expected 1 results not " + result.length, result.length == 1);

    return (Node) result[0].getRootObject();
  }

  protected Node[] fetchGraphsLike(long rootId, String nameWildcard) {
    QStringNode root = createSelect();
    root.where(root.rootId().eq(rootId).and(root.stringField().like(nameWildcard)));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  protected Node[] fetchGraphsBetween(long rootId, String minName, String maxName) {
    QStringNode root = createSelect();
    root.where(root.rootId().eq(rootId).and(root.stringField().between(minName, maxName)));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  protected Node[] fetchGraphsInclusive(long rootId, String minName, String maxName) {
    QStringNode root = createSelect();
    root.where(root.rootId().eq(rootId).and(root.stringField().ge(minName))
        .and(root.stringField().le(maxName)));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

  protected Node[] fetchGraphsExclusive(long rootId, String minName, String maxName) {
    QStringNode root = createSelect();
    root.where(root.rootId().eq(rootId).and(root.stringField().gt(minName))
        .and(root.stringField().lt(maxName)));
    DataGraph[] result = service.find(root);
    assertTrue(result != null);

    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

}
