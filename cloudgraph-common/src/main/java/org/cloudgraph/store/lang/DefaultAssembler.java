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
package org.cloudgraph.store.lang;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.CloudGraphConstants;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.core.CoreNode;
import org.plasma.sdo.core.TraversalDirection;
import org.plasma.sdo.helper.PlasmaDataFactory;
import org.plasma.sdo.profile.KeyType;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;
import commonj.sdo.Property;

/**
 * Supports serial graph assembly. Common graph assembler functionality
 * resulting from initial re-factoring and addition of parallel assembly across
 * RDB and Cassandra services.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public abstract class DefaultAssembler extends AssemblerSupport {
  private static Log log = LogFactory.getLog(DefaultAssembler.class);
  public static final Set<Property> EMPTY_PROPERTY_SET = new HashSet<Property>();
  public static final List<DataObject> EMPTY_DATA_OBJECT_LIST = new ArrayList<DataObject>();
  protected PlasmaType rootType;
  protected PlasmaDataObject root;
  protected Timestamp snapshotDate;
  /**
   * stores a mapping of previously created objects using a PK based hash, such
   * that if the object is discovered again, no graph find is necessary
   */
  protected Map<Integer, PlasmaDataObject> dataObjectMap;
  protected Comparator<PropertyPair> nameComparator;

  public DefaultAssembler(PlasmaType rootType, SelectionCollector collector,
      StatementFactory statementFactory, StatementExecutor statementExecutor,
      Map<Integer, PlasmaDataObject> dataObjectMap, Timestamp snapshotDate) {
    super(collector, statementFactory, statementExecutor);
    this.rootType = rootType;
    this.dataObjectMap = dataObjectMap;
    this.snapshotDate = snapshotDate;

    this.nameComparator = new Comparator<PropertyPair>() {
      @Override
      public int compare(PropertyPair o1, PropertyPair o2) {
        return o1.getProp().getName().compareTo(o2.getProp().getName());
      }
    };
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
  protected abstract void assemble(PlasmaType targetType, PlasmaDataObject source,
      PlasmaProperty sourceProperty, List<PropertyPair> childKeyPairs, int level);

  /**
   * Initiates the assembly of a data graph based on the given results list.
   * 
   * @param results
   *          the results list
   * 
   * @see DataGraphAssembler.getDataGraph()
   */
  public abstract void assemble(List<PropertyPair> results);

  protected DataGraph initRoot(List<PropertyPair> results) {
    DataGraph dataGraph = PlasmaDataFactory.INSTANCE.createDataGraph();
    this.root = (PlasmaDataObject) dataGraph.createRootObject(this.rootType);
    if (log.isDebugEnabled())
      log.debug("assembling root: " + this.root.getType().getName());

    CoreNode rootNode = (CoreNode) this.root;
    // add concurrency fields
    rootNode.setValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP, snapshotDate);
    rootNode.getValueObject().put(CloudGraphConstants.GRAPH_NODE_THREAD_NAME,
        Thread.currentThread().getName());
    // set data properties
    for (PropertyPair pair : results) {
      if (pair.getProp().getType().isDataType()) {
        rootNode.setValue(pair.getProp().getName(), pair.getValue());
      }
    }

    // map it
    int key = createHashKey((PlasmaType) this.root.getType(), results);
    if (log.isDebugEnabled())
      log.debug("mapping root " + key + "->" + this.root);
    this.dataObjectMap.put(key, this.root);

    return dataGraph;
  }

  public Map<PlasmaDataObject, List<PropertyPair>> collectResults(PlasmaType targetType,
      PlasmaDataObject source, PlasmaProperty sourceProperty, List<List<PropertyPair>> result) {
    // first create (or link existing) data objects
    // "filling out" the containment hierarchy at this traversal level
    // BEFORE recursing, as we may "cancel" out an object
    // at the current level if it is first encountered
    // within the recursion.
    Map<PlasmaDataObject, List<PropertyPair>> resultMap = new HashMap<PlasmaDataObject, List<PropertyPair>>();
    for (List<PropertyPair> row : result) {

      PlasmaDataObject target = this.findDataObject(targetType, row);
      // if no existing data-object in graph
      if (target == null) {
        target = this.createDataObject(row, source, sourceProperty);
        resultMap.put(target, row); // add only new object for later
        // traversal
      } else {
        this.link(target, source, sourceProperty);
        continue; // don't map it for later traversal
        // Assume we traverse no farther given no traversal
        // direction or containment info. We only know that we
        // encountered an existing node. Need more path specific
        // info including containment and traversal direction to
        // construct
        // a directed graph here.
        // Since the current selection collector maps any and all
        // properties selected to a type, for each type/data-object
        // we will, at this point, have gotten all the properties we
        // expect anyway.
        // So we create a link from the source to the existing DO, but
        // traverse no further.
      }
    }
    return resultMap;
  }

  /**
   * Creates a new data object contained by the given source data object and
   * source property.
   * 
   * @param row
   *          the results row
   * @param source
   *          the source data object
   * @param sourceProperty
   *          the source containment property
   * @return the new data object
   */
  protected PlasmaDataObject createDataObject(List<PropertyPair> row, PlasmaDataObject source,
      PlasmaProperty sourceProperty) {

    PlasmaDataObject target = (PlasmaDataObject) source.createDataObject(sourceProperty);
    CoreNode node = (CoreNode) target;
    if (log.isDebugEnabled())
      log.debug("create: " + source.getType().getName() + "." + sourceProperty.getName() + "->"
          + target.getType().getName());

    // add concurrency fields
    node.setValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP, snapshotDate);
    node.getValueObject().put(CloudGraphConstants.GRAPH_NODE_THREAD_NAME,
        Thread.currentThread().getName());

    // set data properties bypassing SDO "setter" API
    // to avoid triggering read-only property error
    for (PropertyPair pair : row) {
      if (pair.getProp().getType().isDataType()) {
        if (log.isDebugEnabled())
          log.debug("set: (" + pair.getValue() + ") "
              + pair.getProp().getContainingType().getName() + "." + pair.getProp().getName());
        node.setValue(pair.getProp().getName(), pair.getValue());
      }
    }

    // map it
    int key = createHashKey((PlasmaType) target.getType(), row);
    if (log.isDebugEnabled())
      log.debug("mapping " + key + "->" + target);
    this.dataObjectMap.put(key, target);

    return target;
  }

  /**
   * Finds and returns an existing data object based on the given results row
   * which is part if this assembly unit, or returns null if not exists
   * 
   * @param type
   *          the target type
   * @param row
   *          the results row
   * @return the data object
   */
  protected PlasmaDataObject findDataObject(PlasmaType type, List<PropertyPair> row) {
    int key = createHashKey(type, row);
    PlasmaDataObject result = this.dataObjectMap.get(key);
    if (log.isDebugEnabled()) {
      if (result != null)
        log.debug("found existing mapping " + key + "->" + result);
      else
        log.debug("found no existing mapping for hash key: " + key);
    }
    return result;
  }

  /**
   * Creates a unique mappable key using the qualified type name and all key
   * property values from the given row.
   * 
   * @param type
   *          the type
   * @param row
   *          the data values
   * @return the key
   */
  protected int createHashKey(PlasmaType type, List<PropertyPair> row) {
    PropertyPair[] pairs = new PropertyPair[row.size()];
    row.toArray(pairs);
    Arrays.sort(pairs, this.nameComparator);
    int pkHash = type.getQualifiedName().hashCode();
    int fallBackHash = type.getQualifiedName().hashCode();

    int pks = 0;
    for (int i = 0; i < pairs.length; i++) {
      Object value = pairs[i].getValue();
      if (value == null) {
        log.warn("null voue for property, " + pairs[i].getProp().toString());
        continue;
      }
      if (pairs[i].getProp().isKey(KeyType.primary)) {
        pkHash = pkHash ^ value.hashCode();
        fallBackHash = fallBackHash ^ value.hashCode();
        pks++;
      } else {
        fallBackHash = fallBackHash ^ value.hashCode();
      }
    }
    if (pks > 0) {
      List<Property> pkProps = type.findProperties(KeyType.primary);
      if (pkProps.size() == pks)
        return pkHash;
    }

    return fallBackHash;
  }

  /**
   * Creates a directed (link) between the given source and target data objects.
   * The reference is created as a containment reference only if the given
   * target has no container.
   * 
   * @param target
   *          the data object which is the target
   * @param source
   *          the source data object
   * @param sourceProperty
   *          the source property
   * 
   * @see TraversalDirection
   */
  protected void link(PlasmaDataObject target, PlasmaDataObject source,
      PlasmaProperty sourceProperty) {
    if (log.isDebugEnabled())
      log.debug("linking source (" + source.getUUIDAsString() + ") " + source.getType().getURI()
          + "#" + source.getType().getName() + "." + sourceProperty.getName() + "->("
          + target.getUUIDAsString() + ") " + target.getType().getURI() + "#"
          + target.getType().getName());

    if (sourceProperty.isMany()) {

      PlasmaProperty opposite = (PlasmaProperty) sourceProperty.getOpposite();
      if (opposite != null && !opposite.isMany() && target.isSet(opposite)) {
        PlasmaDataObject existingOpposite = (PlasmaDataObject) target.get(opposite);
        if (existingOpposite != null) {
          log.warn("encountered existing opposite (" + existingOpposite.getType().getName()
              + ") value found while creating link " + source.toString() + "."
              + sourceProperty.getName() + "->" + target.toString() + " - no link created");
          return;
        }
      }
      @SuppressWarnings("unchecked")
      List<DataObject> list = source.getList(sourceProperty);
      if (list == null)
        list = EMPTY_DATA_OBJECT_LIST;

      if (!list.contains(target)) {
        // check if any existing list members already have the opposite
        // property set
        for (DataObject existing : list) {
          if (opposite != null && !opposite.isMany() && existing.isSet(opposite)) {
            PlasmaDataObject existingOpposite = (PlasmaDataObject) existing.get(opposite);
            if (existingOpposite != null) {
              log.warn("encountered existing opposite (" + existingOpposite.getType().getName()
                  + ") value found while creating link " + source.toString() + "."
                  + sourceProperty.getName() + "->" + target.toString() + " - no link created");
              return;
            }
          }
        }

        if (log.isDebugEnabled())
          log.debug("adding target " + source.toString() + "." + sourceProperty.getName() + "->"
              + target.toString());
        if (target.getContainer() == null) {
          target.setContainer(source);
          target.setContainmentProperty(sourceProperty);
        }
        list.add(target);
        source.setList(sourceProperty, list);
        // FIXME: replaces existing list according to SDO spec (memory
        // churn)
        // store some temp instance-property list on DO and only set
        // using SDO
        // API on completion of graph.
      }
    } else {
      // Selection map keys are paths from the root entity and
      // elements in the path are often repeated. Expect repeated
      // events for repeated path elements, which
      // may be useful for some implementations, but not this one. So
      // we screen these out here.
      PlasmaDataObject existing = (PlasmaDataObject) source.get(sourceProperty);
      if (existing == null) {
        source.set(sourceProperty, target);
        // While the SDO spec seems to indicate (see 3.1.6 Containment)
        // that
        // a Type may have only 1 reference property which a containment
        // property, this seems too inflexible given the almost infinite
        // number of ways a graph could be constructed. We therefore
        // allow any reference
        // property to be a containment property, and let the graph
        // assembly
        // order determine which properties are containment properties
        // for a particular
        // graph result. The SDO spec is crystal clear that every Data
        // Object
        // other than the root, must have one-and-only-one container. We
        // set the container
        // here as well as the specific reference property that
        // currently is
        // the containment property, based on graph traversal order.
        // Note it would be
        // possible to specify exactly which property is containment in
        // a
        // query specification. We set no indication of containment on
        // the
        // (source) container object because all reference properties
        // are
        // potentially containment properties.
        if (target.getContainer() == null) {
          target.setContainer(source);
          target.setContainmentProperty(sourceProperty);
        }
      } else if (!existing.equals(target))
        if (log.isDebugEnabled())
          log.debug("encountered existing (" + existing.getType().getName()
              + ") value found while creating link " + source.toString() + "."
              + sourceProperty.getName() + "->" + target.toString());
    }
  }

  public PlasmaDataGraph getDataGraph() {
    return (PlasmaDataGraph) this.root.getDataGraph();
  }

  public void clear() {
    this.root = null;
    this.dataObjectMap.clear();
  }

}
