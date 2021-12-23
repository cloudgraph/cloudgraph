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
package org.cloudgraph.hbase.slice;

import java.io.IOException;
import java.util.Calendar;
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
 * Date SDO datatype specific graph slice test.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public class DatePredicateSliceTest extends DataTypeGraphModelTest {
  private static Log log = LogFactory.getLog(DatePredicateSliceTest.class);
  private String USERNAME = "slice_test";
  private int WAIT_TIME = 1050;

  public static Test suite() {
    return PlasmaTestSetup.newTestSetup(DatePredicateSliceTest.class);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testEqual() throws IOException {
    int id = Integer.valueOf(String.valueOf(System.nanoTime()).substring(7)) + 10000; // space
                                                                                      // tests
                                                                                      // out
                                                                                      // more;
    // than 1 second
    Calendar now = Calendar.getInstance();
    now.setTime(new Date(id));
    now.clear(Calendar.MILLISECOND);
    Node root = this.createGraph(id, now.getTime());
    service.commit(root.getDataGraph(), USERNAME);

    // fetch a slice
    Node fetched = this.fetchSliceEquals(now.getTime());
    debugGraph(fetched.getDataGraph());
    assertNotNull(fetched);
    assertTrue(fetched.getChildCount() == 1);
    assertNotNull(fetched.getChild(0).getDateField());
    assertTrue(fetched.getChild(0).getDateField().compareTo(now.getTime()) == 0);
  }

  public void testGreaterThan() throws IOException {
    int id = Integer.valueOf(String.valueOf(System.nanoTime()).substring(7)) + 20000; // space
                                                                                      // tests
                                                                                      // out
                                                                                      // more;
    // than 1 second
    Calendar now = Calendar.getInstance();
    now.setTime(new Date(id));
    now.clear(Calendar.MILLISECOND);
    Node root = this.createGraph(id, now.getTime());
    service.commit(root.getDataGraph(), USERNAME);

    // fetch a slice
    Node fetched = this.fetchSliceGreaterThan(now.getTime());
    assertNotNull(fetched);
    debugGraph(fetched.getDataGraph());
    assertTrue(fetched.getChildCount() == maxRows - 1);
    for (Node node : fetched.getChild())
      assertTrue(node.getDateField().compareTo(now.getTime()) > 0);
  }

  public void testBetween() throws IOException {
    int id = Integer.valueOf(String.valueOf(System.nanoTime()).substring(7)) + 30000; // space
                                                                                      // tests
                                                                                      // out
                                                                                      // more;
    // than 1 second
    Calendar now = Calendar.getInstance();
    now.setTime(new Date(id));
    now.clear(Calendar.MILLISECOND);
    Node root = this.createGraph(id, now.getTime());
    service.commit(root.getDataGraph(), USERNAME);

    int startIndex = 1;
    int endIndex = 4;

    Calendar start = Calendar.getInstance();
    start.setTime(now.getTime());
    start.clear(Calendar.MILLISECOND);
    start.add(Calendar.DAY_OF_YEAR, startIndex);

    Calendar end = Calendar.getInstance();
    end.setTime(now.getTime());
    end.clear(Calendar.MILLISECOND);
    end.add(Calendar.DAY_OF_YEAR, endIndex);

    log.info(start.getTime());
    log.info(end.getTime());

    // fetch a slice
    Node fetched = this.fetchSliceBetween(now.getTime(), start.getTime(), end.getTime());
    assertNotNull(fetched);
    debugGraph(fetched.getDataGraph());
    assertTrue(fetched.getChildCount() == (endIndex - startIndex) + 1);
    assertNotNull(fetched.getChild(startIndex - 1).getDateField());
    assertTrue(fetched.getChild(startIndex - 1).getDateField().compareTo(start.getTime()) == 0);
    assertTrue(fetched.getChild(endIndex - 1).getDateField().compareTo(end.getTime()) == 0);
  }

  public void testLike() throws IOException {
    int id = Integer.valueOf(String.valueOf(System.nanoTime()).substring(7)) + 40000; // space
                                                                                      // tests
                                                                                      // out
                                                                                      // more;
    // than 1 second
    Calendar now = Calendar.getInstance();
    now.setTime(new Date(id));
    now.clear(Calendar.MILLISECOND);
    Node root = this.createGraph(id, now.getTime());
    service.commit(root.getDataGraph(), USERNAME);

    // fetch a slice
    Node fetched = this.fetchSliceLike(now.getTime(), "*date*", "*3");
    assertNotNull(fetched);
    debugGraph(fetched.getDataGraph());
    assertTrue(fetched.getChildCount() == 1);
    assertNotNull(fetched.getChild(0).getStringField());
    assertTrue(fetched.getChild(0).getStringField().contains("date"));
    assertTrue(fetched.getChild(0).getStringField().contains("3"));
  }

  protected DateNode createGraph(int id, Date now) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(DateNode.class);
    DateNode root = (DateNode) dataGraph.createRootObject(rootType);
    fillNode(root, id, now, "date", 0, 0);
    fillGraph(root, id, now, "date");
    return root;
  }

  protected Node fetchSliceEquals(Date date) {
    QDateNode root = QDateNode.newQuery();
    QDateNode node = QDateNode.newQuery();
    Expression predicate = node.dateField().eq(date).and(node.levelNum().eq(1));
    createSelect(root, predicate);

    root.where(root.dateField().eq(date));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Node) result[0].getRootObject();
  }

  protected Node fetchSliceGreaterThan(Date date) {
    QDateNode root = QDateNode.newQuery();
    QDateNode node = QDateNode.newQuery();
    Expression predicate = node.dateField().gt(date).and(node.levelNum().eq(1));
    createSelect(root, predicate);

    root.where(root.dateField().eq(date));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Node) result[0].getRootObject();
  }

  protected Node fetchSliceBetween(Date date, Date start, Date end) {
    QDateNode root = QDateNode.newQuery();
    QDateNode node = QDateNode.newQuery();
    Expression predicate = node.levelNum().eq(1).and(node.dateField().between(start, end));

    createSelect(root, predicate);

    root.where(root.dateField().eq(date));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Node) result[0].getRootObject();
  }

  protected Node fetchSliceLike(Date date, String wildcard1, String wildcard2) {
    QDateNode root = QDateNode.newQuery();
    QDateNode node = QDateNode.newQuery();

    // group + left expr
    Expression predicate = node.group(
        node.stringField().like(wildcard1).and(node.stringField().like(wildcard2))).and(
        node.levelNum().eq(1));

    // right expr + group
    // Expression predicate = node.levelNum().eq(22)
    // .and(node
    // .group(node.stringField().like(wildcard1)
    // .and(node.stringField().like(wildcard2))));

    // Expression predicate = node.stringField().like(wildcard2)
    // .and(node.stringField().like(wildcard1))
    // .and(node.levelNum().eq(1));
    createSelect(root, predicate);

    root.where(root.dateField().eq(date));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Node) result[0].getRootObject();
  }

  private QDateNode createSelect(QDateNode root, Expression predicate) {
    root.select(root.dateField());
    root.select(root.child(predicate).dateField());
    root.select(root.child(predicate).stringField());
    root.select(root.child(predicate).levelNum());
    root.select(root.child(predicate).child(predicate).dateField());
    root.select(root.child(predicate).child(predicate).child(predicate).dateField());
    root.select(root.child(predicate).child(predicate).child(predicate).child(predicate)
        .dateField());
    return root;
  }
}
