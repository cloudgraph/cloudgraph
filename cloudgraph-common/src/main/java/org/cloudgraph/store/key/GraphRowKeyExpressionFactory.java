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
