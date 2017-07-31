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
package org.cloudgraph.query.expr;

/**
 * Simple visitor client which creates a "printable" representation of a binary
 * expression tree for debugging purposes.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * 
 * @see Expr
 */
public class ExprPrinter implements ExprVisitor {
  StringBuilder buf = new StringBuilder();

  @Override
  public void visit(Expr target, Expr source, int level) {
    buf.append("\n");
    for (int i = 0; i < level; i++)
      buf.append("\t");
    buf.append(target.toString());

  }

  public String toString() {
    return buf.toString();
  }
}
