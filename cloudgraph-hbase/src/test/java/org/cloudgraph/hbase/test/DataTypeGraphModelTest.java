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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.test.datatypes.Node;
import org.cloudgraph.test.datatypes.query.QNode;
import org.plasma.query.Expression;
import org.plasma.sdo.helper.DataConverter;

import org.apache.jena.ext.com.google.common.primitives.UnsignedInteger;
import org.apache.jena.ext.com.google.common.primitives.UnsignedLong;

import commonj.sdo.DataGraph;
import commonj.sdo.Property;

/**
 * @author Scott Cinnamond
 * @since 0.5
 */
public abstract class DataTypeGraphModelTest extends HBaseTestCase {
  private static Log log = LogFactory.getLog(DataTypeGraphModelTest.class);

  protected int maxLevels = 1;
  protected int maxRows = 6; // Some tests slice for name on third child, so

  // keep at least 3 rows

  protected void fillGraph(Node root, int id, Date now, String namePrefix) {
    Node parent = root;
    for (int i = 0; i < maxRows; i++) {
      Node child = parent.createChild();
      fillNode(child, id, now, namePrefix, 1, i);
      /*
       * for (int j = 0; j < maxRows; j++) { Node child2 = child.createChild();
       * fillNode(child2, id, now, namePrefix, 2, j); for (int k = 0; k <
       * maxRows; k++) { Node child3 = child2.createChild(); fillNode(child3,
       * id, now, namePrefix, 3, k); } }
       */
    }
    // addNodes(root, id, now,
    // maxLevels, 1,
    // maxRows);
  }

  protected void addNodes(Node parent, int id, Date now, String namePrefix, long maxLevels,
      long level, long maxRows) {

    for (int i = 0; i < maxRows; i++) {
      Node child = parent.createChild();
      fillNode(child, id, now, namePrefix, level, i);
      if (level < maxLevels)
        addNodes(child, id, now, namePrefix, maxLevels, level++, maxRows);
    }
  }

  protected Node fillNode(Node node, int id, Date now, String namePrefix, long level, long sequence) {
    return fillNode(node, id, id, now, namePrefix, level, sequence);
  }

  protected Node fillNode(Node node, int rootId, int id, Date now, String namePrefix, long level,
      long sequence) {
    String name = namePrefix + "_" + level + "_" + sequence;
    return this.fillNodeSimple(node, rootId, id, now, name, level, sequence);
  }

  protected Node fillNodeSimple(Node node, int rootId, int id, Date now, String name, long level,
      long sequence) {
    long temp = id - (long) 1357000000000L;
    // log.info("id: " + id);
    // log.info("reduced id: " + temp);

    double doubleId = (double) temp * (double) 0.001;
    float floatId = Double.valueOf(doubleId).floatValue();

    node.setId(UUID.randomUUID().toString());
    node.setRootId(rootId);
    node.setLevelNum(level);
    node.setSequenceNum(sequence);

    node.setName(name);
    node.setBooleanField(true);
    node.setByteField((byte) 1);
    node.setBytesField(name.getBytes());
    node.setCharacterField('c');

    Calendar cal = Calendar.getInstance();
    cal.setTime(now);
    cal.add(Calendar.DAY_OF_YEAR, (int) sequence);

    node.setDateField(cal.getTime());

    Property prop = node.getType().getProperty(Node.DATE_TIME_FIELD);
    node.setDateTimeField((String) DataConverter.INSTANCE.fromDate(prop.getType(), cal.getTime()));

    prop = node.getType().getProperty(Node.DAY_FIELD);
    node.setDayField((String) DataConverter.INSTANCE.fromDate(prop.getType(), cal.getTime()));

    prop = node.getType().getProperty(Node.MONTH_FIELD);
    node.setMonthField((String) DataConverter.INSTANCE.fromDate(prop.getType(), cal.getTime()));

    prop = node.getType().getProperty(Node.MONTH_DAY_FIELD);
    node.setMonthDayField((String) DataConverter.INSTANCE.fromDate(prop.getType(), cal.getTime()));

    prop = node.getType().getProperty(Node.YEAR_FIELD);
    node.setYearField((String) DataConverter.INSTANCE.fromDate(prop.getType(), cal.getTime()));

    prop = node.getType().getProperty(Node.YEAR_MONTH_FIELD);
    node.setYearMonthField((String) DataConverter.INSTANCE.fromDate(prop.getType(), cal.getTime()));

    prop = node.getType().getProperty(Node.YEAR_MONTH_DAY_FIELD);
    node.setYearMonthDayField((String) DataConverter.INSTANCE.fromDate(prop.getType(),
        cal.getTime()));

    prop = node.getType().getProperty(Node.TIME_FIELD);
    node.setTimeField((String) DataConverter.INSTANCE.fromDate(prop.getType(), cal.getTime()));

    node.setDecimalField(new BigDecimal(doubleId));
    node.setDoubleField(doubleId);
    // node.setDurationField();
    node.setFloatField(floatId);

    node.setIntField(Integer.MAX_VALUE);
    node.setUnsignedIntField(UnsignedInteger.valueOf(Integer.MAX_VALUE));
    node.setIntegerField(BigInteger.valueOf(id));

    node.setLongField(id);
    node.setUnsignedLongField(UnsignedLong.valueOf(id));

    node.setObjectField(name);
    node.setShortField(Short.MAX_VALUE);
    node.setStringField(name);
    List<String> list = new ArrayList<String>();
    list.add(name);
    node.setStringsField(list);
    node.setUriField(name);
    return node;

  }

  protected Node fetchGraphFull(long id) {
    QNode query = createGraphQueryFull(id);
    this.marshal(query.getModel(), id);

    DataGraph[] result = service.find(query);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Node) result[0].getRootObject();
  }

  protected QNode createGraphQueryFull(long id) {
    QNode root = QNode.newQuery();
    root.select(root.wildcard());
    root.select(root.child().wildcard());
    root.select(root.child().child().wildcard());
    root.select(root.child().child().child().wildcard());
    root.select(root.child().child().child().child().wildcard());

    root.where(root.rootId().eq(id));
    // root.where(root.creationDate().between(new Date(), new Date()));
    return root;
  }

  protected Node fetchGraphSlice(long id, String name) {
    QNode root = QNode.newQuery();
    Expression predicate = root.name().eq(name);
    root.select(root.wildcard());
    root.select(root.child(predicate).wildcard());
    root.select(root.child(predicate).child().wildcard());
    root.select(root.child(predicate).child().child().wildcard());
    root.select(root.child(predicate).child().child().child().wildcard());

    root.where(root.rootId().eq(id));

    DataGraph[] result = service.find(root);
    assertTrue(result != null);
    assertTrue(result.length == 1);

    return (Node) result[0].getRootObject();
  }

}