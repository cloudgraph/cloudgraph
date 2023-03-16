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
package org.cloudgraph.rocksdb.scan;

/**
 * Represents the literal for an individual field within a composite partial row
 * key scan and provides access to start and stop byte sequences for various
 * wildcard operators under the HBase partial row-key scan for various
 * optionally configurable hashing, formatting, padding and other features.
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 * @see PartialRowKey
 * @see FuzzyRowKeyLiteral
 */
public interface WildcardPartialRowKeyLiteral extends PartialRowKeyLiteral {

  /**
   * Returns the "start row" bytes used under certain conditions to represent a
   * wildcard operator under an HBase partial row-key scan under the various
   * optionally configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used under certain conditions to represent a
   *         wildcard operator under an HBase partial row-key scan under the
   *         various optionally configurable hashing, formatting and padding
   *         features.
   */
  public byte[] getBetweenStartBytes();

  /**
   * Returns the "stop row" bytes used under certain conditions to represent a
   * wildcard operator under an HBase partial row-key scan under the various
   * optionally configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used under certain conditions to represent a
   *         wildcard operator under an HBase partial row-key scan under the
   *         various optionally configurable hashing, formatting and padding
   *         features.
   */
  public byte[] getBetweenStopBytes();
}
