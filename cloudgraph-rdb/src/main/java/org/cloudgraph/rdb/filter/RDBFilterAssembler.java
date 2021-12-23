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
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.lang.FilterAssembler;
import org.cloudgraph.store.service.AliasMap;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.QueryException;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.model.AbstractPathElement;
import org.plasma.query.model.Expression;
import org.plasma.query.model.From;
import org.plasma.query.model.Function;
import org.plasma.query.model.Literal;
import org.plasma.query.model.Path;
import org.plasma.query.model.PathElement;
import org.plasma.query.model.Property;
import org.plasma.query.model.Query;
import org.plasma.query.model.QueryConstants;
import org.plasma.query.model.Where;
import org.plasma.query.model.PredicateOperator;
import org.plasma.query.model.WildcardPathElement;
import org.plasma.query.visitor.Traversal;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.access.provider.common.SQLQueryFilterAssembler;
import org.plasma.sdo.helper.PlasmaTypeHelper;
import org.plasma.sdo.profile.KeyType;
import org.xml.sax.SAXException;

import commonj.sdo.Type;

public class RDBFilterAssembler extends SQLQueryFilterAssembler implements QueryConstants,
    FilterAssembler {
  private static Log log = LogFactory.getLog(RDBFilterAssembler.class);

  private Map variableMap;
  private StringBuffer variableDecls;
  private String importDecls;
  private String parameterDecls;
  private int variableDeclCount = 0;
  private int subqueryCount = 0;

  private AliasMap aliasMap;

  public RDBFilterAssembler(Where where, Type contextType, AliasMap aliasMap) {
    super(contextType);
    this.aliasMap = aliasMap;

    if (where.getTextContent() == null && where.getFilterId() == null) {
      if (where.getImportDeclaration() != null)
        throw new DataAccessException(
            "import declaration allowed only for 'free-text' Where clause");
      if (where.getParameters().size() > 0)
        throw new DataAccessException("parameters allowed only for 'free-text' Where clause");
      if (where.getParameterDeclaration() != null)
        throw new DataAccessException(
            "parameter declarations allowed only for 'free-text' Where clause");
      if (where.getVariableDeclaration() != null)
        throw new DataAccessException(
            "import declarations allowed only for 'free-text' Where clause");

      if (log.isDebugEnabled())
        log(where);
      this.filter.append(" WHERE ");
      where.accept(this); // traverse
    } else {
      for (int i = 0; i < where.getParameters().size(); i++)
        params.add(where.getParameters().get(i).getValue());

      if (where.getImportDeclaration() != null)
        importDecls = where.getImportDeclaration().getValue();
      if (where.getParameterDeclaration() != null)
        parameterDecls = where.getParameterDeclaration().getValue();
      if (where.getVariableDeclaration() != null) {
        if (variableDecls == null)
          variableDecls = new StringBuffer();
        variableDecls.append(where.getVariableDeclaration().getValue());
      }
      if (where.getTextContent() != null) {
        filter.append(where.getTextContent().getValue());
      } else
        throw new QueryException("expected free-text content or filter id");
    }
  }

  public AliasMap getAliasMap() {
    return aliasMap;
  }

  public String getVariableDeclarations() {
    return variableDecls.toString();
  }

  public boolean hasVariableDeclarations() {
    return variableDecls != null && variableDecls.length() > 0;
  }

  public String getImportDeclarations() {
    return importDecls;
  }

  public boolean hasImportDeclarations() {
    return importDecls != null && importDecls.length() > 0;
  }

  public String getParameterDeclarations() {
    return parameterDecls;
  }

  public boolean hasParameterDeclarations() {
    return parameterDecls != null && parameterDecls.length() > 0;
  }

  public void start(Expression expression) {
    for (int i = 0; i < expression.getTerms().size(); i++) {
      PredicateOperator predicateOper = expression.getTerms().get(i).getPredicateOperator();
      if (predicateOper != null) {
        if (isSubquery(predicateOper)) {
          Property property = expression.getTerms().get(i - 1).getProperty();
          Query query = (Query) expression.getTerms().get(i + 1).getQuery();
          assembleSubquery(property, predicateOper, query);
          subqueryCount++;
          this.getContext().setTraversal(Traversal.ABORT);
          // abort traversal as vanilla expression
        }
      }
    }
  }

  private boolean isSubquery(PredicateOperator predicateOper) {
    switch (predicateOper.getValue()) {
    case IN:
    case NOT_IN:
    case EXISTS:
    case NOT_EXISTS:
      return true;
    default:
      return false;
    }
  }

  protected void assembleSubquery(Property property, PredicateOperator oper, Query query) {
    From from = query.getFromClause();
    Type type = PlasmaTypeHelper.INSTANCE.getType(from.getEntity().getNamespaceURI(), from
        .getEntity().getName());
    String alias = ALIAS_PREFIX + String.valueOf(subqueryCount);
    SubqueryFilterAssembler assembler = new SubqueryFilterAssembler(alias, query, params, type);

    if (property.getPath() != null)
      throw new QueryException("properties with paths (" + property.getName()
          + ") not allowed as subquery target");

    commonj.sdo.Property endpointProperty = contextType.getProperty(property.getName());

    if (endpointProperty.isMany())
      throw new QueryException("multi-valued properties (" + contextType.getName() + "."
          + endpointProperty.getName() + ") not allowed as subquery target");
    contextProperty = endpointProperty;

    switch (oper.getValue()) {
    case IN:
      filter.append("(");
      filter.append(assembler.getFilter());
      filter.append(").contains(");
      filter.append(DATA_ACCESS_CLASS_MEMBER_PREFIX + endpointProperty.getName());
      filter.append(")");
      break;
    case NOT_IN:
      filter.append("!(");
      filter.append(assembler.getFilter());
      filter.append(").contains(");
      filter.append(DATA_ACCESS_CLASS_MEMBER_PREFIX + endpointProperty.getName());
      filter.append(")");
      break;
    case EXISTS:
      filter.append("!("); // negate it
      filter.append(assembler.getFilter());
      filter.append(").isEmpty()");
      break;
    case NOT_EXISTS:
      filter.append("(");
      filter.append(assembler.getFilter());
      filter.append(").isEmpty()");
      break;
    }
  }

  protected void processWildcardExpression(Property property, PredicateOperator oper,
      Literal literal) {
    String content = literal.getValue().trim();
    content = content.replace(WILDCARD, "%");
    start(property);
    filter.append("'");
    filter.append(content);
    filter.append("'");
  }

  /**
   * Handles a property query node, traversing the property path appending SQL
   * 'AND' expressions based on key relationships until the property endpoint is
   * reached. Superclass handlers deal with other query nodes such as operators
   * and literals.
   */
  @Override
  public void start(Property property) {

    Path path = property.getPath();

    if (filter.length() > 0)
      filter.append(" ");

    PlasmaType targetType = (PlasmaType) contextType;
    String targetAlias = this.aliasMap.getAlias(targetType);
    if (targetAlias == null)
      targetAlias = this.aliasMap.addAlias(targetType);

    if (path != null) {

      String pathKey = "";
      for (int i = 0; i < path.getPathNodes().size(); i++) {
        PlasmaType prevTargetType = targetType;
        String prevTargetAlias = targetAlias;

        AbstractPathElement pathElem = path.getPathNodes().get(i).getPathElement();
        if (pathElem instanceof WildcardPathElement)
          throw new DataAccessException(
              "wildcard path elements applicable for 'Select' clause paths only, not 'Where' clause paths");
        String elem = ((PathElement) pathElem).getValue();
        PlasmaProperty prop = (PlasmaProperty) targetType.getProperty(elem);

        targetType = (PlasmaType) prop.getType(); // traverse
        targetAlias = this.aliasMap.getAlias(targetType);
        if (targetAlias == null)
          targetAlias = this.aliasMap.addAlias(targetType);

        pathKey += "/" + elem;

        if (!prop.isMany()) {
          filter.append(prevTargetAlias + "." + prop.getPhysicalName());
          filter.append(" = ");
          PlasmaProperty priKeyProp = (PlasmaProperty) targetType.findProperty(KeyType.primary);
          filter.append(targetAlias + "." + priKeyProp.getPhysicalName());
        } else {
          PlasmaProperty opposite = (PlasmaProperty) prop.getOpposite();
          if (opposite.isMany())
            throw new DataAccessException("expected singular opposite for property, "
                + prop.getContainingType().getURI() + "#" + prop.getContainingType().getName()
                + "." + prop.getName());
          filter.append(targetAlias + "." + opposite.getPhysicalName());
          filter.append(" = ");
          PlasmaProperty priKeyProp = (PlasmaProperty) prevTargetType.findProperty(KeyType.primary);
          filter.append(prevTargetAlias + "." + priKeyProp.getPhysicalName());
        }

        filter.append(" AND ");
      }
    }

    // process endpoint
    PlasmaProperty endpointProp = (PlasmaProperty) targetType.getProperty(property.getName());
    contextProperty = endpointProp;

    // start functions
    List<Function> functions = property.getFunctions();
    if (functions == null || functions.size() == 0) {
      filter.append(targetAlias + "." + endpointProp.getPhysicalName());
    } else {
      filter.append(Functions.wrap(endpointProp, functions, targetAlias));
    }

    super.start(property);
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
    log.debug("where: " + xml);
  }
}