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

import org.plasma.sdo.core.CoreConstants;

/**
 * Enumeration based qualifier or code-set used to describe a graph/row.
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
public enum GraphMetaKey implements MetaKey {

  /**
   * The timestamp for an entire graph indicating the last modified date for the
   * graph, the value being a string representation of a long integer.
   */
  TIMESTAMP("__TS__", "timestamp for an entire graph"),

  /**
   * The sequence mapping. A serialized sequence mapping the state of which is
   * unmarshalled and used as a sequence generator.
   */
  SEQUENCE_MAPPING("__SM__", "the sequence mapping state"),

  /**
   * The tombstone marker a graph.
   */
  TOMBSTONE("__TN__", "the tombstone marker for a graph");

  private String code;
  private String description;
  byte[] codeBytes;

  private GraphMetaKey(String code, String description) {
    this.code = code;
    this.description = description;
    this.codeBytes = this.code.getBytes(Charset.forName(CoreConstants.UTF8_ENCODING));
  }

  @Override
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
  public String asString() {
    return this.name();
  }
}
