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

import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * Assembles a complete composite or partial composite partial row (start/stop)
 * key pair where each field within the composite start and stop row keys are
 * constructed based a set of query predicates.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public interface RowKeyScanAssembler {
  /**
   * Assemble row key scan information based on one or more given query
   * predicates.
   * 
   * @param where
   *          the where predicate hierarchy
   * @param contextType
   *          the context type which may be the root type or another type linked
   *          by one or more relations to the root
   */
  public void assemble(Where where, PlasmaType contextType);

  /**
   * Assemble row key scan information based only on the data graph root type
   * information such as the URI and type logical or physical name.
   */
  public void assemble();

  /**
   * Assemble row key scan information based on the given scan literals.
   * 
   * @param literals
   *          the scan literals
   */
  public void assemble(ScanLiterals literals);
}
