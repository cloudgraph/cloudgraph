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
package org.cloudgraph.aerospike.scan;

/**
 * Represents the literal for an individual field within a composite row key and
 * provides access to the key byte sequence for various supported relational
 * operators under a row-key 'Get' operation for various optionally configurable
 * hashing, formatting, padding and other features.
 * 
 * @author Scott Cinnamond
 * @since 0.5.5
 * @see CompleteRowKeyScan
 * @see CompleteRowKeyLiteral
 */
public interface CompleteRowKeyLiteral {

  /**
   * Returns the bytes used to represent an "equals" relational operator for a
   * specific composite row key field, under an HBase 'Get' operation for the
   * various optionally configurable hashing, formatting and padding features.
   * 
   * @return the bytes used to represent an "equals" relational operator for a
   *         specific composite row key field, under an HBase 'Get' operation
   *         for the various optionally configurable hashing, formatting and
   *         padding features.
   */
  public byte[] getEqualsBytes();

}
