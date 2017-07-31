/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.rdb.filter;

// java imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.service.AliasMap;
import org.plasma.query.model.GroupBy;
import org.plasma.query.model.Path;
import org.plasma.query.model.Property;
import org.plasma.query.model.QueryConstants;
import org.plasma.query.visitor.DefaultQueryVisitor;
import org.plasma.query.visitor.Traversal;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

public class RDBGroupingAssembler extends DefaultQueryVisitor implements QueryConstants {
  private static Log log = LogFactory.getLog(RDBGroupingAssembler.class);

  private PlasmaType contextType;
  private commonj.sdo.Property contextProp;
  private StringBuilder groupingDeclaration = new StringBuilder();
  private AliasMap aliasMap;

  @SuppressWarnings("unused")
  private RDBGroupingAssembler() {
  }

  public RDBGroupingAssembler(GroupBy groupby, PlasmaType contextType, AliasMap aliasMap) {
    this.contextType = contextType;
    this.aliasMap = aliasMap;

    if (groupby.getTextContent() == null)
      groupby.accept(this); // traverse
    else
      groupingDeclaration.append(groupby.getTextContent().getValue());
  }

  public String getGroupingDeclaration() {
    return groupingDeclaration.toString();
  }

  public void start(Property property) {
    if (groupingDeclaration.length() == 0)
      groupingDeclaration.append("GROUP BY ");

    if (groupingDeclaration.length() > "GROUP BY ".length())
      groupingDeclaration.append(", ");
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

    String alias = this.aliasMap.getAlias(targetType);
    if (alias == null)
      alias = this.aliasMap.addAlias(targetType);
    groupingDeclaration.append(alias);
    groupingDeclaration.append(".");
    groupingDeclaration.append(endpoint.getPhysicalName());
    this.getContext().setTraversal(Traversal.ABORT);
  }
}