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
package org.cloudgraph.rdb.filter;

// java imports
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.query.QueryException;
import org.plasma.query.model.AbstractPathElement;
import org.plasma.query.model.AbstractProperty;
import org.plasma.query.model.Literal;
import org.plasma.query.model.Path;
import org.plasma.query.model.PathElement;
import org.plasma.query.model.Property;
import org.plasma.query.model.Query;
import org.plasma.query.model.Select;
import org.plasma.query.model.SubqueryOperator;
import org.plasma.query.model.Where;
import org.plasma.query.model.WildcardOperator;
import org.plasma.query.model.WildcardPathElement;
import org.plasma.query.visitor.Traversal;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.access.provider.common.SQLQueryFilterAssembler;

import commonj.sdo.Type;

public class SubqueryFilterAssembler extends SQLQueryFilterAssembler {
  private static Log log = LogFactory.getLog(SubqueryFilterAssembler.class);

  private String alias;
  private Object[] filterParams;
  private int variableCount = 0;

  public SubqueryFilterAssembler(String alias, Query query, List params, Type contextType) {
    super(contextType, params);
    this.alias = alias;
    this.contextType = contextType;
    this.filterParams = filterParams;
    query.accept(this);
  }

  public void start(Select select) {
    if (select.getProperties().size() > 1)
      throw new QueryException("multiple properties on subqueries not supported");
    if (select.getProperties().size() == 0)
      throw new QueryException("found no properties on subquery");
    AbstractProperty property = select.getProperties().get(0);
    if (!(property instanceof Property))
      throw new QueryException("properties of type '" + property.getClass().getSimpleName()
          + "' not supported on subquery 'select'");
    if (property.getPath() != null)
      throw new QueryException("property paths not supported on subquery 'select'");

    commonj.sdo.Property prop = contextType.getProperty(((Property) property).getName());
    if (!prop.getType().isDataType())
      throw new QueryException("reference properties (" + contextType.getName() + "."
          + prop.getName() + ") not supported on subquery 'select'");
    if (prop.isMany())
      throw new QueryException("multi-valued properties (" + contextType.getName() + "."
          + prop.getName() + ") not supported on subquery 'select'");

    filter.append("select ");

    filter.append(alias + ".");
    filter.append(DATA_ACCESS_CLASS_MEMBER_PREFIX + prop.getName() + " ");
    filter.append("from org.plasma.sdo.das.pom." + contextType.getName() + " " + alias);

    this.getContext().setTraversal(Traversal.ABORT);
    // abort further traversal
  }

  public void start(Where where) {
    filter.append(" where ");
    super.start(where); // continue QOM traversal
  }

  @Override
  public void start(Property property) {
    if (log.isDebugEnabled()) {
      log.debug("visit Property, " + property.getName());
    }

    Path path = property.getPath();

    if (filter.length() > 0)
      filter.append(" " + alias + ".");

    // EntityDef targetEntityDef = contextType;
    Type targetType = contextType;
    if (path != null) {
      for (int i = 0; i < path.getPathNodes().size(); i++) {
        AbstractPathElement pathElem = path.getPathNodes().get(i).getPathElement();
        if (pathElem instanceof WildcardPathElement)
          throw new DataAccessException(
              "wildcard path elements applicable for 'Select' clause paths only, not 'Where' clause paths");

        commonj.sdo.Property pdef = targetType.getProperty(((PathElement) pathElem).getValue());

        targetType = pdef.getOpposite().getContainingType();

        if (!pdef.isMany()) {
          filter.append(DATA_ACCESS_CLASS_MEMBER_PREFIX + pdef.getName());
          filter.append(".");
        } else {
          String variableName = alias + String.valueOf(variableCount);
          filter.append(DATA_ACCESS_CLASS_MEMBER_PREFIX + pdef.getName()
              + DATA_ACCESS_CLASS_MEMBER_MULTI_VALUED_SUFFIX + ".contains(" + variableName
              + ") && " + variableName + ".");
          variableCount++;
        }
      }
    }
    // PropertyDef endpoint = mom.findPropertyDef(targetEntityDef,
    // property.getName());
    commonj.sdo.Property endpoint = targetType.getProperty(property.getName());
    contextProperty = endpoint;
    filter.append(DATA_ACCESS_CLASS_MEMBER_PREFIX + endpoint.getName());

    super.start(property);
  }

  @Override
  protected void assembleSubquery(Property property, SubqueryOperator oper, Query query) {
    // TODO Auto-generated method stub

  }

  @Override
  protected void processWildcardExpression(Property property, WildcardOperator oper, Literal literal) {
    // TODO Auto-generated method stub

  }

}