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
import org.cloudgraph.core.key.CompositeColumnKeyFactory;
//import org.cloudgraph.rocksdb.key.CompositeColumnKeyFactory;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

/**
 * Creates an HBase value and qualifier filter hierarchy to return HBase scan
 * results representing a graph "slice". Accommodates slice predicates where 2
 * or more columns (qualifiers) are involved. In such cases the qualifier
 * filters must be grouped together in an 'OR' filter list <a target="#" href=
 * "https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ONE"
 * >MUST_PASS_ONE</a> and the value filters must, for example be grouped
 * separately an another filter list.
 * 
 * Uses <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/QualifierFilter.html"
 * >QualifierFilter</a> /<a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/ValueFilter.html"
 * >ValueFilter</a> pairs recreating composite column qualifier prefixes using
 * {@link CompositeColumnKeyFactory}. Processes visitor events for query model
 * elements specific to assembly of HBase column filters, such as properties,
 * wildcards, literals, logical operators, relational operators, within the
 * context of HBase filter hierarchy assembly. Maintains various context
 * information useful to subclasses.
 * <p>
 * HBase filters may be collected into lists using <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html"
 * target="#">FilterList</a> each with a <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ALL"
 * target="#">MUST_PASS_ALL</a> or <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ONE"
 * target="#">MUST_PASS_ONE</a> (logical) operator. Lists may then be assembled
 * into hierarchies used to represent complex expression trees filtering either
 * rows or columns in HBase.
 * </p>
 * 
 * @see org.cloudgraph.common.key.CompositeColumnKeyFactory
 * @see org.cloudgraph.rocksdb.filter.MultiColumnPredicateVisitor
 * @author Scott Cinnamond
 * @since 0.6.5
 */
public class MultiColumnPredicateFilterAssembler extends MultiColumnPredicateVisitor implements
    PredicateFilterAssembler {
  private static Log log = LogFactory.getLog(MultiColumnPredicateFilterAssembler.class);

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
  public MultiColumnPredicateFilterAssembler(PlasmaType rootType, ServiceContext serviceContext) {
    super(rootType);

    this.columnKeyFac = new CompositeColumnKeyFactory(rootType, serviceContext.getStoreMapping());
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
