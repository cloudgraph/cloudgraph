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
package org.cloudgraph.store.key;

import java.util.List;

import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;
import commonj.sdo.Type;

/**
 * Generates an data store row key based on the configured CloudGraph row key
 * {@link org.cloudgraph.config.RowKeyModel model} for a specific HTable
 * {@link org.cloudgraph.config.Table configuration}.
 * <p>
 * The initial creation and subsequent reconstitution for query retrieval
 * purposes of both row and column keys in CloudGraph&#8482; is efficient, as it
 * leverages byte array level API in both Java and the current underlying SDO
 * 2.1 implementation, <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a>. Both composite row and
 * column keys are composed in part of structural metadata, and the lightweight
 * metadata API within <a target="#"
 * href="http://plasma-sdo.org">PlasmaSDO&#8482;</a> contains byte-array level,
 * cached lookup of all basic metadata elements including logical and physical
 * type and property names.
 * </p>
 * 
 * @see org.cloudgraph.config.RowKeyModel
 * @see org.cloudgraph.config.Table
 * @author Scott Cinnamond
 * @since 0.5
 */
public interface GraphRowKeyFactory {

  /**
   * Creates and returns a composite row key based on properties within the
   * given data graph using its root data object as the graph root.
   * Implementations are typically driven by the CloudGraph configuration
   * section specified for the given Data Graph URI and Type name.
   * 
   * @param dataGraph
   *          the Data Graph
   * @return a composite row key.
   */
  public byte[] createRowKeyBytes(DataGraph dataGraph);

  /**
   * Creates and returns a composite row key based on properties within the data
   * graph using the given data object as the graph root. Implementations are
   * typically driven by the CloudGraph configuration section specified for the
   * given Data Graph URI and Type name.
   * 
   * @param dataGraph
   *          the Data Graph
   * @return a composite row key.
   */
  public byte[] createRowKeyBytes(DataObject dataObject);

  /**
   * Generates a row key based only on the given root type.
   * 
   * @param type
   *          the root type for the target data graph
   * @return the row key
   */
  public byte[] createRowKeyBytes(Type type);

  /**
   * Creates and returns a composite row key based on the given key values.
   * Implementations are typically driven by the CloudGraph configuration
   * section specified for the given Data Graph URI and Type name.
   * 
   * @param values
   *          the key values
   * @return a composite row key.
   */
  public byte[] createRowKeyBytes(List<KeyValue> values);
}
