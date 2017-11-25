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
package org.cloudgraph.hbase.key;

import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.sdo.PlasmaType;

/**
 * A key factory which helps implementations leverage table and data graph
 * specific configuration information, such as the hashing algorithm and field
 * level row and column model settings.
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
 * @see org.cloudgraph.store.mapping.StoreMapping
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.store.mapping.DataGraphMapping
 * @author Scott Cinnamond
 * @since 0.5
 */
public interface ConfigurableKeyFactory {
  public TableMapping getTable();

  public DataGraphMapping getGraph();

  public PlasmaType getRootType();
}
