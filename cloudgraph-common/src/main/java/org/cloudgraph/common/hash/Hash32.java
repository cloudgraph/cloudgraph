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
package org.cloudgraph.common.hash;

/**
 * Uses a 32 bit hash algorithm and returns a 32 bit integer hash
 * 
 * @author Scott Cinnamond
 */
public interface Hash32 {

  /**
   * Calculate a hash using all bytes from the input argument, and a seed of -1.
   * 
   * @param bytes
   * @return the hash value
   */
  public abstract int hash(byte[] bytes);

  /**
   * Calculate a hash using bytes from 0 to length, and the provided seed value.
   * 
   * @param bytes
   * @param length
   * @param initval
   * @return the hash value
   */
  public abstract int hash(byte[] bytes, int initval);

  public abstract int hash(byte[] data, int offset, int length);

}