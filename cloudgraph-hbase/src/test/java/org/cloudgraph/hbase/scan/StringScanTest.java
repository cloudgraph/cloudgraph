/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
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
  protected long WAIT_TIME = 1;
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

  protected StringNode createGraph(long rootId, long id, Date now, String prefix) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    dataGraph.getChangeSummary().beginLogging(); // log changes from this
    // point
    Type rootType = PlasmaTypeHelper.INSTANCE.getType(StringNode.class);
    StringNode root = (StringNode) dataGraph.createRootObject(rootType);
    fillNode(root, rootId, id, now, prefix, 0, 0);
    fillGraph(root, id, now, prefix);
    return root;
  }
}
