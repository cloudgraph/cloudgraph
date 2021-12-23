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

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.hbase.test.DataTypeGraphModelTest;
import org.cloudgraph.test.datatypes.StringNode;
import org.cloudgraph.test.datatypes.query.QStringNode;
import org.plasma.query.Expression;
import org.plasma.sdo.helper.PlasmaDataFactory;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.DataGraph;
import commonj.sdo.Type;

/**
 * String SDO datatype specific partial row-key scan operations test.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */

public abstract class StringScanTest extends DataTypeGraphModelTest {
  private static Log log = LogFactory.getLog(StringScanTest.class);
  protected int WAIT_TIME = 1;
  protected String USERNAME = "string_test";

  protected QStringNode createSelect(String name) {
    QStringNode root = QStringNode.newQuery();
    Expression predicate = root.name().eq(name);
    root.select(root.wildcard());
    root.select(root.child(predicate).wildcard());
    root.select(root.child(predicate).child().wildcard());
    root.select(root.child(predicate).child().child().wildcard());
    root.select(root.child(predicate).child().child().child().wildcard());
    return root;
  }

  protected QStringNode createSelect() {
    QStringNode root = QStringNode.newQuery();
    root.select(root.wildcard());
    root.select(root.child().wildcard());
    root.select(root.child().child().wildcard());
    root.select(root.child().child().child().wildcard());
    root.select(root.child().child().child().child().wildcard());
    return root;
  }

  protected StringNode createGraph(int rootId, int id, Date now, String prefix) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(StringNode.class);
    StringNode root = (StringNode) dataGraph.createRootObject(rootType);
    fillNode(root, rootId, id, now, prefix, 0, 0);
    fillGraph(root, id, now, prefix);
    return root;
  }

  protected StringNode createSimpleGraph(int rootId, int id, Date now, String name) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(StringNode.class);
    StringNode root = (StringNode) dataGraph.createRootObject(rootType);
    fillNodeSimple(root, rootId, id, now, name, 0, 0);
    return root;
  }
}
