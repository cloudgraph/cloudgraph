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
