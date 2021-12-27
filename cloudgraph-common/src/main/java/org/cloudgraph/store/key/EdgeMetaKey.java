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

import java.nio.charset.Charset;

import org.plasma.sdo.DataType;
import org.plasma.sdo.core.CoreConstants;

/**
 * Enumeration based qualifier or code-set used to describe a set or collection
 * of edges used to find and de-reference local or remote entities.
 * <p>
 * </p>
 * The enumeration name is referenced directly in source code, and the
 * underlying code is used as the actual physical column qualifier. The allows
 * the source to remain more readable while allowing more terse physical
 * qualifiers or column keys.
 *
 * @author Scott Cinnamond
 * @since 1.0.0
 */
public enum EdgeMetaKey implements MetaKey {

  /**
   * The timestamp for an edge/collection indicating the last modified date for
   * the edge, the value being a string representation of a long integer.
   */
  TIMESTAMP("_TS_",
      "timestamp for an edge/collection indicating the last modified date for the edge",
      DataType.String),

  /**
   * This field specifies the base entity type for the (abstract) base class for
   * the reference collection. The value associated with this field is a
   * qualified type identifier.
   */
  BASETYPE("_BT_",
      "specifies the base entity type for the (abstract) base class for the reference collection",
      DataType.String),

  /**
   * The default entity subtype for elements within an edge or reference
   * collection. The default sybtype is only created if the actual entity type
   * differs from the base entity type. If valued it does not require all future
   * entities within the collection to be instances of the sybtype.
   */
  SUBTYPE("_ST_", "default entity subtype for elements within an edge or reference collection",
      DataType.String),

  /**
   * This field specifies the path to the collection within a remote graph. This
   * path is used to locate target entities anywhere within a non-local target
   * graph and is essential for reference cleanup during delete operations. The
   * value associated with this field is a path composed of physical property
   * names from the graph root to the target collection.
   */
  PATH("_PH_", "specifies the path to the collection within a remote graph", DataType.String),

  /**
   * This field specifies the count of entities associated with the collection.
   */
  COUNT("_CT_", "specifies the count of entities associated with the collection", DataType.String),

  /**
   * This field specifies the entity sequence numbers for the target entities
   * contained in the target collection. The sequence numbers within the value
   * for this field are added during insert operations and removed during delete
   * operations. All such reference metadata mutations must be associated and
   * kept in sync with mutations on the actual target entities.
   */
  SEQUENCES("_SQ_",
      "entity sequence numbers for the target entities contained in the target collection",
      DataType.String),

  /**
   * This field specifies the row keys associated with the target entities for
   * the reference collection, where the targets are not part of the local
   * graph, but are found in another row or table.
   */
  ROWKEYS("_RK_", "row keys associated with the target entities for the reference collection",
      DataType.Bytes);

  private String code;
  private String description;
  byte[] codeBytes;
  DataType storageType;

  private EdgeMetaKey(String code, String description, DataType storageType) {
    this.code = code;
    this.description = description;
    this.codeBytes = this.code.getBytes(Charset.forName(CoreConstants.UTF8_ENCODING));
    this.storageType = storageType;
  }

  public byte[] codeAsBytes() {
    return this.codeBytes;
  }

  @Override
  public String code() {
    return this.code;
  }

  @Override
  public String description() {
    return this.description;
  }

  @Override
  public DataType getStorageType() {
    return this.storageType;
  }

  @Override
  public String asString() {
    return this.name() + " (" + this.code + ")";
  }
}
