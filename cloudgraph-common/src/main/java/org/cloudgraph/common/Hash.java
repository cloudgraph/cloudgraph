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
package org.cloudgraph.common;

/**
 * @author Scott Cinnamond
 * @since 0.5
 */
public interface Hash {
  /**
   * Calculate a hash using all bytes from the input argument, and a seed of -1.
   * Uses the hash algorithm defined for this specific HBase table or if no
   * defined, uses the hash algorithm set for the HBase configuration.
   * 
   * @param bytes
   * @return the hash value
   */
  public int hash(byte[] bytes);

  /**
   * Calculate a hash using all bytes from the input argument, and a provided
   * seed value. Uses the hash algorithm defined for this specific HBase table
   * or if no defined, uses the hash algorithm set for the HBase configuration.
   * 
   * @param bytes
   * @param initval
   * @return the hash value
   */
  public int hash(byte[] bytes, int initval);

  /**
   * Calculate a hash using bytes from 0 to length, and the provided seed value.
   * Uses the hash algorithm defined for this specific HBase table or if no
   * defined, uses the hash algorithm set for the HBase configuration.
   * 
   * @param bytes
   * @param length
   * @param initval
   * @return the hash value
   */
  public int hash(byte[] bytes, int length, int initval);

}
