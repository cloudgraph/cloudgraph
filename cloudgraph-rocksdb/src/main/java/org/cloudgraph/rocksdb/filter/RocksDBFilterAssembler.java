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
package org.cloudgraph.rocksdb.filter;

import org.cloudgraph.common.Assembler;

/**
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public interface RocksDBFilterAssembler extends Assembler {

  /**
   * Returns the assembled filter, filter list or filter hierarchy root.
   * 
   * @return the assembled filter, filter list or or filter hierarchy root.
   */
  public Filter getFilter();

}