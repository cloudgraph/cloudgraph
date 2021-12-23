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
 * String SDO datatype specific fuzzy row-key scan operations test.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */

public class StringFuzzyRowKeyScanTest extends StringScanTest {
  private static Log log = LogFactory.getLog(StringFuzzyRowKeyScanTest.class);

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(StringFuzzyRowKeyScanTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testWildcard() throws IOException {
    int rootId = Integer.valueOf(String.valueOf(System.nanoTime()).substring(7));
    int id = WAIT_TIME;
    Date now = new Date(rootId);
    Node root = this.createGraph(rootId, id, now, "XYZ_A");
    service.commit(root.getDataGraph(), USERNAME);

    // create 2 more w/same id but new date
    int id2 = id + WAIT_TIME;
    Date now2 = new Date(id2);
    Node root2 = this.createGraph(rootId, id2, now2, "XYZ_B");
    service.commit(root2.getDataGraph(), USERNAME);

    int id3 = id2 + WAIT_TIME;
    Date now3 = new Date(id3);
    Node root3 = this.createGraph(rootId, id3, now3, "XYZ_C");
    service.commit(root3.getDataGraph(), USERNAME);

    // fetch
    Node[] fetched = this.fetchGraphs(rootId, "XYZ*");
    assertTrue(fetched != null);
    assertTrue(fetched.length == 3);
    for (Node node : fetched) {
      String xml = serializeGraph(node.getDataGraph());
      log.debug("GRAPH: " + xml);
      assertTrue(node.getRootId() == rootId);
    }
  }

  protected Node[] fetchGraphs(long rootId, String wildcard) {
    QStringNode root = createSelect();

    // use name only which causes a fuzzy search
    root.where(root.rootId().eq(rootId).and(root.stringField().like(wildcard)));

    this.marshal(root.getModel(), rootId);
    DataGraph[] result = service.find(root);
    Node[] profiles = new Node[result.length];
    for (int i = 0; i < result.length; i++)
      profiles[i] = (Node) result[i].getRootObject();
    return profiles;
  }

}
