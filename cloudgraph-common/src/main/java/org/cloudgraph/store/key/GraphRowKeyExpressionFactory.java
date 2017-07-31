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

import commonj.sdo.Type;

/**
 * Generates a row key based on the configured CloudGraph row key
 * {@link org.cloudgraph.config.RowKeyModel model} for a specific table
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
public interface GraphRowKeyExpressionFactory {

  /**
   * Generates a regular expression matching a row key based on the given list
   * of token value pairs.
   * 
   * @param values
   *          the token value list
   * @return the row key expression
   */
  public String createRowKeyExpr(List<KeyValue> values);

  public byte[] createRowKeyExprBytes(List<KeyValue> values);

  /**
   * Returns true if the data graph configured for the given
   * {@link commonj.sdo.Type type} has a user defined token which maps to the
   * given property path.
   * 
   * @param type
   *          the SDO type
   * @param path
   *          the property path
   * @return true if the data graph configured for the given
   *         {@link commonj.sdo.Type type} has a user defined token which maps
   *         to the given property path.
   */
  public boolean hasUserDefinedRowKeyToken(Type type, String path);
}
