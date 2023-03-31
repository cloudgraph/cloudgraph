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
package org.cloudgraph.core.scan;

/**
 * Represents the literal for an individual field within a composite partial row
 * key scan and provides access to start and stop byte sequences for various
 * relational operators under the HBase partial row-key scan for various
 * optionally configurable hashing, formatting, padding and other features.
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see PartialRowKey
 * @see FuzzyRowKeyLiteral
 */
public interface PartialRowKeyLiteral {

  /**
   * Returns the HBase scan "start-row" composite row-key field bytes for this
   * literal under the various relational, logical operator and other optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the HBase scan "start-row" composite row-key field bytes for this
   *         literal under the various relational, logical operator and other
   *         optionally configurable hashing, formatting and padding features.
   */
  public byte[] getStartBytes();

  /**
   * Returns the HBase scan "stop-row" composite row-key field bytes for this
   * literal under the various relational, logical operator and other optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the HBase scan "stop-row" composite row-key field bytes for this
   *         literal under the various relational, logical operator and other
   *         optionally configurable hashing, formatting and padding features.
   */
  public byte[] getStopBytes();

  /**
   * Returns the "start row" bytes used to represent "equals" relational
   * operator under an HBase partial row-key scan under the various optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "equals" relational
   *         operator under an HBase partial row-key scan under the various
   *         optionally configurable hashing, formatting and padding features.
   */
  public byte[] getEqualsStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "equals" relational operator
   * under an HBase partial row-key scan under the various optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "equals" relational operator
   *         under an HBase partial row-key scan under the various optionally
   *         configurable hashing, formatting and padding features.
   */
  public byte[] getEqualsStopBytes();

  /**
   * Returns the "start row" bytes used to represent "greater than" relational
   * operator under an HBase partial row-key scan under the various optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "greater than" relational
   *         operator under an HBase partial row-key scan under the various
   *         optionally configurable hashing, formatting and padding features.
   */
  public byte[] getGreaterThanStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "greater than" relational
   * operator under an HBase partial row-key scan under the various optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "greater than" relational
   *         operator under an HBase partial row-key scan under the various
   *         optionally configurable hashing, formatting and padding features.
   */
  public byte[] getGreaterThanStopBytes();

  /**
   * Returns the "start row" bytes used to represent "greater than equals"
   * relational operator under an HBase partial row-key scan under the various
   * optionally configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "greater than equals"
   *         relational operator under an HBase partial row-key scan under the
   *         various optionally configurable hashing, formatting and padding
   *         features.
   */
  public byte[] getGreaterThanEqualStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "greater than equals"
   * relational operator under an HBase partial row-key scan under the various
   * optionally configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "greater than equals"
   *         relational operator under an HBase partial row-key scan under the
   *         various optionally configurable hashing, formatting and padding
   *         features.
   */
  public byte[] getGreaterThanEqualStopBytes();

  /**
   * Returns the "start row" bytes used to represent "less than" relational
   * operator under an HBase partial row-key scan under the various optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "less than" relational
   *         operator under an HBase partial row-key scan under the various
   *         optionally configurable hashing, formatting and padding features.
   */
  public byte[] getLessThanStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "less than" relational
   * operator under an HBase partial row-key scan under the various optionally
   * configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "less than" relational
   *         operator under an HBase partial row-key scan under the various
   *         optionally configurable hashing, formatting and padding features.
   */
  public byte[] getLessThanStopBytes();

  /**
   * Returns the "start row" bytes used to represent "less than equals"
   * relational operator under an HBase partial row-key scan under the various
   * optionally configurable hashing, formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "less than equals"
   *         relational operator under an HBase partial row-key scan under the
   *         various optionally configurable hashing, formatting and padding
   *         features.
   */
  public byte[] getLessThanEqualStartBytes();

  /**
   * Returns the "stop row" bytes used to represent "less than equals"
   * relational operator under an HBase partial row-key scan under the various
   * optionally configurable hashing, formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "less than equals"
   *         relational operator under an HBase partial row-key scan under the
   *         various optionally configurable hashing, formatting and padding
   *         features.
   */
  public byte[] getLessThanEqualStopBytes();

}
