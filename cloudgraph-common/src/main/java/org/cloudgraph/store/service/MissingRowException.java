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
package org.cloudgraph.store.service;

/**
 * Exception that indicates a row was not found but expected within a given
 * table.
 * 
 * <p>
 * Note that copied data objects have the same data properties as the source but
 * have new (and therefore different) underlying <a target="#"
 * href="http://docs.oracle.com/javase/6/docs/api/java/util/UUID.html">UUID</a>
 * and other management properties which are not defined within the source Type.
 * Use copied data objects to help automate and save save effort when creating
 * <b>NEW</b> data objects. To simply link/add and existing data object to a new
 * data graph, first use {@link DataObject.detach()} to remove it from its
 * graph. Than add it to a another graph.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class MissingRowException extends GraphServiceException {
  private static final long serialVersionUID = 1L;

  /**
   * Constructor.
   * 
   * @param table
   *          the name of the table for the expected row
   * @param rowKey
   *          the row key
   */
  public MissingRowException(String table, String rowKey) {
    super("expected row for key '" + rowKey + "' for table '" + table
        + "' - note: if the composite row key includes a UUID "
        + "and a copied data object was used (CopyHelper creates a new UUID), "
        + "the row will not be found - use DataObject.detach(), then add "
        + "the existing detached data object");
  }

}