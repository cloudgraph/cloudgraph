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

/**
 * The configuration for a column key.
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class ColumnKeyFieldMapping extends MetaKeyFieldMapping {

  private ColumnKeyField columnKeyField;

  public ColumnKeyFieldMapping(DataGraphMapping dataGraph, ColumnKeyField field, int seqNum,
      int totalFields) {
    super(dataGraph, field, seqNum, totalFields);
    this.columnKeyField = field;
  }
}
