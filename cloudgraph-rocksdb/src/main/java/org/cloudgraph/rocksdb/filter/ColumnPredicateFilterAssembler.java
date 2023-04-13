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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.key.StatefullColumnKeyFactory;
//import org.cloudgraph.rocksdb.key.CompositeColumnKeyFactory;
//import org.cloudgraph.rocksdb.key.StatefullColumnKeyFactory;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * Creates an Aerospike value and qualifier filter hierarchy to return Aerospike
 * scan results representing part of a graph "slice".
 * 
 * 
 * @see org.cloudgraph.common.key.CompositeColumnKeyFactory
 * @author Scott Cinnamond
 * @since 0.5
 */
public class ColumnPredicateFilterAssembler extends ColumnPredicateVisitor implements
    PredicateFilterAssembler {
  private static Log log = LogFactory.getLog(ColumnPredicateFilterAssembler.class);

  /**
   * Constructor which takes a {@link org.plasma.query.model.Query query} where
   * clause containing any number of predicates and traverses these as a
   * {org.plasma.query.visitor.QueryVisitor visitor} only processing various
   * traversal events as needed against the given root type.
   * 
   * @param where
   *          where the where clause
   * @param contextType
   *          the current SDO type
   * @param rootType
   *          the graph root type
   * @see org.plasma.query.visitor.QueryVisitor
   * @see org.plasma.query.model.Query
   */
  public ColumnPredicateFilterAssembler(PlasmaType rootType, ServiceContext serviceContext) {
    super(rootType);
    this.columnKeyFac = new StatefullColumnKeyFactory(rootType, serviceContext);
  }

  @Override
  public void assemble(Where where, PlasmaType contextType) {
    this.contextType = contextType;
    for (int i = 0; i < where.getParameters().size(); i++)
      params.add(where.getParameters().get(i).getValue());

    if (log.isDebugEnabled())
      this.log(where);

    if (log.isDebugEnabled())
      log.debug("begin traverse");

    where.accept(this); // traverse

    if (log.isDebugEnabled())
      log.debug("end traverse");
  }

  public void clear() {
    super.clear();
  }

}
