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
package org.cloudgraph.aerospike.filter;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.aerospike.key.CompositeRowKeyExpressionFactory;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;
import org.xml.sax.SAXException;

/**
 * Processes a <a
 * href="http://docs.plasma-sdo.org/api/org/plasma/query/model/Where.html"
 * target="#">where</a> predicate <a href=
 * "http://plasma-sdo.org/alldocs/plasma/api/org/plasma/query/model/Expression.html"
 * target="#">expression</a> tree into a set of HBase <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/RowFilter.html"
 * target="#">row filters</a> arranged within a hierarchy of HBase <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html"
 * target="#">filter lists</a>. The resulting <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html"
 * target="#">filter list</a> resembles the given expression tree with AND/OR <a
 * href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ALL"
 * target="#">MUST_PASS_ALL</a>/<a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.Operator.html#MUST_PASS_ONE"
 * target="#">MUST_PASS_ONE</a> semantics representing the input. A <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/FilterList.html"
 * target="#">filter list</a> stack is maintained which mirrors the query <a
 * href="http://docs.plasma-sdo.org/api/org/plasma/query/model/Expression.html"
 * target="#">expression</a> being processed.
 * 
 * @see CompositeRowKeyExpressionFactory
 * @see org.cloudgraph.store.key.GraphRowKeyFactory
 * @see org.plasma.query.visitor.QueryVisitor
 * @see org.plasma.query.model.Query
 * @author Scott Cinnamond
 * @since 0.5
 */
public class PredicateRowFilterAssembler extends RowPredicateVisitor implements
    PredicateFilterAssembler {
  private static Log log = LogFactory.getLog(PredicateRowFilterAssembler.class);

  /**
   * Constructor sets up a {@link CompositeRowKeyExpressionFactory} for the
   * given root type.
   * 
   * @param rootType
   *          the root type
   * @see CompositeRowKeyExpressionFactory
   */
  public PredicateRowFilterAssembler(PlasmaType rootType, StoreMappingContext mappingContext) {
    super(rootType);
    this.rowKeyFac = new CompositeRowKeyExpressionFactory(rootType, mappingContext);
  }

  /**
   * Takes a {@link org.plasma.query.model.Query query} where clause containing
   * any number of predicates and traverses these as a
   * {org.plasma.query.visitor.QueryVisitor visitor} only processing various
   * traversal events as needed against the root type.
   * 
   * @param where
   *          the where clause
   * @param contextType
   *          the context type
   * @see org.plasma.query.visitor.QueryVisitor
   * @see org.plasma.query.model.Query
   */
  @Override
  public void assemble(Where where, PlasmaType contextType) {
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

  protected void log(Where root) {
    String xml = "";
    PlasmaQueryDataBinding binding;
    try {
      binding = new PlasmaQueryDataBinding(new DefaultValidationEventHandler());
      xml = binding.marshal(root);
    } catch (JAXBException e) {
      log.debug(e);
    } catch (SAXException e) {
      log.debug(e);
    }
    log.debug("query: " + xml);
  }
}
