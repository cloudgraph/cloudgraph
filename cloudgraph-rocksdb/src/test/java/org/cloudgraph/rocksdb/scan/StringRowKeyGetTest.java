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
import org.cloudgraph.test.datatypes.Node;
import org.cloudgraph.test.datatypes.query.QStringNode;
import org.plasma.common.test.PlasmaTestSetup;

import commonj.sdo.DataGraph;

/**
 * String SDO datatype specific row-key get operations test.
 * 
 * @author Scott Cinnamond
 * @since 0.5.5
 */

public class StringRowKeyGetTest extends StringScanTest {
  private static Log log = LogFactory.getLog(StringRowKeyGetTest.class);

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(StringRowKeyGetTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testEqual() throws IOException {
    int rootId = Integer.valueOf(String.valueOf(System.nanoTime()).substring(7));
    int id = rootId + WAIT_TIME;
    Date now = new Date(id);
    Node root = this.createGraph(rootId, id, now, "AAA");
    service.commit(root.getDataGraph(), USERNAME);

    // create 2 more w/same id but new date
    int id2 = id + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "BBB");
    service.commit(new DataGraph[] { root2.getDataGraph() }, USERNAME);

    int id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "CCC");
    service.commit(root3.getDataGraph(), USERNAME);

    // fetch
    Node fetched = this.fetchSingleGraph(root2.getRootId(), root2.getStringField());
    String xml = serializeGraph(fetched.getDataGraph());
    log.debug("GRAPH: " + xml);
    assertTrue(fetched.getRootId() == rootId);

  }

  protected Node fetchSingleGraph(long rootId, String name) {
    QStringNode root = createSelect();

    root.where(root.rootId().eq(rootId).and(root.stringField().eq(name)));

    this.marshal(root.getModel(), rootId);

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    // FIXME: failing
    assertTrue("expected 1 results", result.length == 1);

    return (Node) result[0].getRootObject();
  }

}
