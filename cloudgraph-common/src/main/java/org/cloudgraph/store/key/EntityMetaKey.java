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
 * Enumeration based qualifier or code-set used to describe an "entity" or
 * instance of some type within a graph.
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
public enum EntityMetaKey implements MetaKey {
  /**
   * Represents the uuid of an entity, and is a mandatory field for all queries.
   * The value associated with this field is a uuid which allows all assembled
   * data object to be universally unique across sessions and clients
   */
  UUID("_UU", "the UUID for an entity", DataType.String),
  /**
   * The qualified type name for the entity. Used to dynamically determine the
   * type/subtype of the entity e.g. when unmarshalled or de-referenced as part
   * of an edge (collection).
   */
  TYPE("_TP", "qualified type name for an entity", DataType.String);

  private String code;
  private String description;
  byte[] codeBytes;
  DataType storageType;

  private EntityMetaKey(String code, String description, DataType storageType) {
    this.code = code;
    this.description = description;
    this.storageType = storageType;
    this.codeBytes = this.code.getBytes(Charset.forName(CoreConstants.UTF8_ENCODING));
  }

  @Override
  public byte[] codeAsBytes() {
    return this.codeBytes;
  }

  @Override
  public String asString() {
    return this.name() + " (" + this.code + ")";
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
}
