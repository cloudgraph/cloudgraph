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
package org.cloudgraph.recognizer;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprVisitor;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.cloudgraph.query.expr.WildcardBinaryExpr;
import org.plasma.sdo.PlasmaType;

/**
 * A simple query {@link Expr expression} visitor which determines whether a
 * graph recognizer is required, within the context of a binary (query)
 * {@link Expr expression} syntax tree, encapsulating operator precedence and
 * other factors.
 * <p>
 * Visits the expression tree and for each expression determines whether the
 * property and its path are represented within the row key model for the
 * current {@link DataGraphConfig graph} by a user defined
 * {@link UserDefinedRowKeyFieldConfig field}. If not, then the property and its
 * {@link Expr expression} are outside the row key and can't be represented by a
 * scan. Therefore a recognizer is required.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 * @see org.cloudgraph.query.expr.Expr
 * @see org.cloudgraph.query.expr.RelationalBinaryExpr
 * @see org.cloudgraph.query.expr.ExprVisitor
 * @see org.cloudgraph.config.DataGraphConfig
 * @see org.cloudgraph.config.UserDefinedRowKeyFieldConfig
 * @see org.cloudgraph.query.expr.LogicalBinaryExpr
 * @see org.cloudgraph.query.expr.RelationalBinaryExpr
 * @see org.cloudgraph.query.expr.WildcardBinaryExpr
 */
public class GraphRecognizerDetector implements ExprVisitor {

  private static Log log = LogFactory.getLog(GraphRecognizerDetector.class);

  private PlasmaType rootType;
  private DataGraphConfig graph;
  private boolean queryRequiresGraphRecognizer = false;

  public GraphRecognizerDetector(PlasmaType rootType) {
    this.rootType = rootType;
    QName rootTypeQname = this.rootType.getQualifiedName();
    this.graph = CloudGraphConfig.getInstance().getDataGraph(rootTypeQname);
  }

  public boolean isQueryRequiresGraphRecognizer() {
    return queryRequiresGraphRecognizer;
  }

  @Override
  public void visit(Expr target, Expr source, int level) {
    if (target instanceof RelationalBinaryExpr) {
      RelationalBinaryExpr expr = (RelationalBinaryExpr) target;
      UserDefinedRowKeyFieldConfig fieldConfig = graph.getUserDefinedRowKeyField(expr
          .getPropertyPath());
      if (fieldConfig == null) {
        this.queryRequiresGraphRecognizer = true;
        return;
      }
    } else if (target instanceof WildcardBinaryExpr) {
      WildcardBinaryExpr expr = (WildcardBinaryExpr) target;
      UserDefinedRowKeyFieldConfig fieldConfig = graph.getUserDefinedRowKeyField(expr
          .getPropertyPath());
      if (fieldConfig == null) {
        this.queryRequiresGraphRecognizer = true;
        return;
      }
    }
  }

}
