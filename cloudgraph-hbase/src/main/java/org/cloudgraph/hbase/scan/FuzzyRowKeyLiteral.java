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
package org.cloudgraph.hbase.scan;

/**
 * Represents the literal for an individual field within a composite fuzzy row
 * key scan using HBase <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FuzzyRowFilter.html"
 * >FuzzyRowFilter</a>, and provides access to the key byte and fuzzy info byte
 * sequences under various optionally configurable hashing, formatting, padding
 * and other features.
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see PartialRowKey
 * @see FuzzyRowKey
 */
public interface FuzzyRowKeyLiteral {
  public byte[] getFuzzyKeyBytes();

  public byte[] getFuzzyInfoBytes();
}
