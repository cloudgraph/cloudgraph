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
