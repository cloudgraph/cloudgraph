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
package org.cloudgraph.rdb.graph;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.common.concurrent.GraphMetricVisitor;
import org.cloudgraph.rdb.filter.RDBStatementExecutor;
import org.cloudgraph.rdb.filter.RDBStatementFactory;
import org.cloudgraph.store.lang.DefaultAssembler;
import org.cloudgraph.store.lang.LangStoreGraphAssembler;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.core.CoreNode;

import commonj.sdo.DataGraph;
import commonj.sdo.Property;

/**
 * Constructs a data graph starting with a given root SDO type based on a
 * "selection graph" of selected SDO properties, where properties are mapped by
 * selected types required in the result graph.
 * <p>
 * The assembly is triggered by calling the {@link
 * GraphAssembler#assemble(List<PropertyPair> results)} method which initializes
 * the graph root and begins a breadth first traversal of the selection graph as
 * represented in the underlying data store.
 * <p>
 * Various metrics for the assembly are collected using
 * {@link GraphMetricVisitor} and are available as SDO instance properties.
 * 
 * @see GraphMetricVisitor
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public class GraphAssembler extends DefaultAssembler implements LangStoreGraphAssembler {

  private static Log log = LogFactory.getLog(GraphAssembler.class);

  public GraphAssembler(PlasmaType rootType, SelectionCollector collector, Timestamp snapshotDate,
      Connection con) {
    super(rootType, collector, new RDBStatementFactory(), new RDBStatementExecutor(con),
        new HashMap<Integer, PlasmaDataObject>(), snapshotDate);
  }

  /**
   * Initiates the assembly of a data graph based on the given results list.
   * 
   * @param results
   *          the results list
   * 
   * @see DataGraphAssembler.getDataGraph()
   */
  @Override
  public void assemble(List<PropertyPair> results) {

    long before = System.currentTimeMillis();
    DataGraph dataGraph = initRoot(results);
    CoreNode rootNode = (CoreNode) dataGraph.getRootObject();

    // singular reference props
    for (PropertyPair pair : results) {
      if (pair.getProp().isMany() || pair.getProp().getType().isDataType())
        continue;
      List<PropertyPair> childKeyProps = this.getChildKeyPairs(pair);
      assemble((PlasmaType) pair.getProp().getType(), (PlasmaDataObject) this.root, pair.getProp(),
          childKeyProps, 1);
    }

    // multi reference props (not found in results)
    Set<Property> props = this.collector.getProperties(this.rootType);
    for (Property p : props) {
      PlasmaProperty prop = (PlasmaProperty) p;
      if (prop.isMany() && !prop.getType().isDataType()) {
        List<PropertyPair> childKeyProps = this.getChildKeyPairs(root, prop);
        assemble((PlasmaType) prop.getType(), (PlasmaDataObject) this.root, prop, childKeyProps, 1);
      }
    }

    long after = System.currentTimeMillis();

    rootNode.getValueObject().put(CloudGraphConstants.GRAPH_ASSEMBLY_TIME,
        Long.valueOf(after - before));

    GraphMetricVisitor visitor = new GraphMetricVisitor();
    this.root.accept(visitor);

    rootNode.getValueObject().put(CloudGraphConstants.GRAPH_NODE_COUNT,
        Long.valueOf(visitor.getCount()));
    rootNode.getValueObject()
        .put(CloudGraphConstants.GRAPH_DEPTH, Long.valueOf(visitor.getDepth()));
    rootNode.getValueObject().put(CloudGraphConstants.GRAPH_THREAD_COUNT,
        Long.valueOf(visitor.getThreadCount()));
  }

  /**
   * Assembles a data object of the given target type by first forming a query
   * using the given key/property pairs. If an existing data object is mapped
   * for the given key pairs, the existing data object is linked.
   * 
   * @param targetType
   *          the type for the data object to be assembled
   * @param source
   *          the source data object
   * @param sourceProperty
   *          the source property
   * @param childKeyPairs
   *          the key pairs for the data object to be assembled
   */
  protected void assemble(PlasmaType targetType, PlasmaDataObject source,
      PlasmaProperty sourceProperty, List<PropertyPair> childKeyPairs, int level) {
    Set<Property> props = this.collector.getProperties(targetType, level);
    if (props == null)
      props = EMPTY_PROPERTY_SET;

    if (log.isDebugEnabled())
      log.debug(String.valueOf(level) + ":assemble: " + source.getType().getName() + "."
          + sourceProperty.getName() + "->" + targetType.getName() + ": " + props);

    List<List<PropertyPair>> result = this.getPredicateResult(targetType, sourceProperty, props,
        childKeyPairs);

    if (log.isDebugEnabled())
      log.debug(String.valueOf(level) + ":results: " + result.size());

    Map<PlasmaDataObject, List<PropertyPair>> resultMap = this.collectResults(targetType, source,
        sourceProperty, result);

    // now traverse
    Iterator<PlasmaDataObject> iter = resultMap.keySet().iterator();
    while (iter.hasNext()) {
      PlasmaDataObject target = iter.next();
      List<PropertyPair> row = resultMap.get(target);
      // traverse singular results props
      for (PropertyPair pair : row) {
        if (pair.getProp().isMany() || pair.getProp().getType().isDataType())
          continue; // only singular reference props
        if (!pair.isQueryProperty())
          continue; // property is a key or other property not
        // explicitly cited in the source query, don't
        // traverse it

        List<PropertyPair> nextKeyPairs = this.getNextKeyPairs(target, pair, level);

        if (log.isDebugEnabled())
          log.debug(String.valueOf(level) + ":traverse: (" + pair.getProp().isMany() + ") "
              + pair.getProp().toString() + ":" + String.valueOf(pair.getValue()));
        assemble((PlasmaType) pair.getProp().getType(), target, pair.getProp(), nextKeyPairs,
            level + 1);
      }

      // traverse multi props based, not on the results
      // row, but on keys within this data object
      for (Property p : props) {
        PlasmaProperty prop = (PlasmaProperty) p;
        if (!prop.isMany() || prop.getType().isDataType())
          continue; // only many reference props

        List<PropertyPair> childKeyProps = this.getChildKeyProps(target, targetType, prop);
        if (log.isDebugEnabled())
          log.debug(String.valueOf(level) + ":traverse: (" + prop.isMany() + ") " + prop.toString()
              + " - " + childKeyProps.toArray().toString());
        assemble((PlasmaType) prop.getType(), target, prop, childKeyProps, level + 1);
      }
    }
  }

}
