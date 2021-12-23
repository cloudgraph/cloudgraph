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
import org.cloudgraph.store.service.AliasMap;
import org.plasma.query.model.Function;
import org.plasma.query.model.OrderBy;
import org.plasma.query.model.Path;
import org.plasma.query.model.Property;
import org.plasma.query.model.QueryConstants;
import org.plasma.query.visitor.DefaultQueryVisitor;
import org.plasma.query.visitor.Traversal;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

public class RDBOrderingAssembler extends DefaultQueryVisitor implements QueryConstants {
  private static Log log = LogFactory.getLog(RDBOrderingAssembler.class);

  private PlasmaType contextType;
  private commonj.sdo.Property contextProp;
  private StringBuilder orderingDeclaration = new StringBuilder();
  private AliasMap aliasMap;

  @SuppressWarnings("unused")
  private RDBOrderingAssembler() {
  }

  public RDBOrderingAssembler(OrderBy orderby, PlasmaType contextType, AliasMap aliasMap) {
    this.contextType = contextType;
    this.aliasMap = aliasMap;

    if (orderby.getTextContent() == null)
      orderby.accept(this); // traverse
    else
      orderingDeclaration.append(orderby.getTextContent().getValue());
  }

  public String getOrderingDeclaration() {
    return orderingDeclaration.toString();
  }

  public void start(Property property) {
    if (orderingDeclaration.length() == 0)
      orderingDeclaration.append("ORDER BY ");

    if (orderingDeclaration.length() > "ORDER BY ".length())
      orderingDeclaration.append(", ");
    PlasmaType targetType = contextType;
    if (property.getPath() != null) {
      Path path = property.getPath();
      for (int i = 0; i < path.getPathNodes().size(); i++) {
        PlasmaProperty prop = (PlasmaProperty) targetType.getProperty(path.getPathNodes().get(i)
            .getPathElement().getValue());
        targetType = (PlasmaType) prop.getType();
      }
    }
    PlasmaProperty endpoint = (PlasmaProperty) targetType.getProperty(property.getName());
    contextProp = endpoint;

    String targetAlias = this.aliasMap.getAlias(targetType);
    if (targetAlias == null)
      targetAlias = this.aliasMap.addAlias(targetType);

    List<Function> functions = property.getFunctions();
    if (functions == null || functions.size() == 0) {
      orderingDeclaration.append(targetAlias + "." + endpoint.getPhysicalName());
    } else {
      orderingDeclaration.append(Functions.wrap(endpoint, functions, targetAlias));
    }

    if (property.getDirection() == null
        || property.getDirection().ordinal() == org.plasma.query.model.SortDirection.ASC.ordinal())
      orderingDeclaration.append(" ASC");
    else
      orderingDeclaration.append(" DESC");

    this.getContext().setTraversal(Traversal.ABORT);
  }
}