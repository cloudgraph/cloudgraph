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
package org.cloudgraph.store.mapping;

import org.cloudgraph.store.mapping.RowKeyModel;

/**
 * Encapsulates logic related to access of row key model configuration
 * information.
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public class RowKeyModelMapping {

  private RowKeyModel rowKeyModel;

  @SuppressWarnings("unused")
  private RowKeyModelMapping() {
  }

  public RowKeyModelMapping(RowKeyModel rowKeyModel) {
    super();
    this.rowKeyModel = rowKeyModel;
  }

  public int getRowKeyFieldCount() {
    return rowKeyModel.rowKeyFields.size();
  }
}