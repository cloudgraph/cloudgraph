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
package org.cloudgraph.hbase.graph;

import org.cloudgraph.hbase.io.RowReader;

/**
 * Associates a row reader with a given graph (traversal) level. For use in
 * detecting a target row reader based on its level.
 * 
 * @see org.cloudgraph.hbase.io.RowReader
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class ReaderFrame {
  private RowReader rowReader;
  private int level;

  public ReaderFrame(RowReader rowReader, int level) {
    super();
    this.rowReader = rowReader;
    this.level = level;
  }

  public RowReader getRowReader() {
    return rowReader;
  }

  public int getLevel() {
    return level;
  }
}
