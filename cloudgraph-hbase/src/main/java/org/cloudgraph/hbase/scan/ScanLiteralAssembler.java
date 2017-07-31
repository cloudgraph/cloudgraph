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
package org.cloudgraph.hbase.scan;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.model.AbstractPathElement;
import org.plasma.query.model.GroupOperator;
import org.plasma.query.model.Literal;
import org.plasma.query.model.LogicalOperator;
import org.plasma.query.model.NullLiteral;
import org.plasma.query.model.Path;
import org.plasma.query.model.PathElement;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.RelationalOperatorValues;
import org.plasma.query.model.Where;
import org.plasma.query.model.WildcardOperator;
import org.plasma.query.model.WildcardPathElement;
import org.plasma.query.visitor.DefaultQueryVisitor;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Assembles the set of data "flavor" and data type specific scan literals used
 * to construct composite partial row (start/stop) key pair.
 * 
 * @see org.cloudgraph.config.DataGraphConfig
 * @see org.cloudgraph.config.TableConfig
 * @author Scott Cinnamond
 * @since 0.5
 */
public class ScanLiteralAssembler extends DefaultQueryVisitor {
  private static Log log = LogFactory.getLog(ScanLiteralAssembler.class);
  protected PlasmaType rootType;
  protected PlasmaType contextType;
  protected PlasmaProperty contextProperty;
  protected String contextPropertyPath;
  protected RelationalOperator contextRelationalOperator;
  protected LogicalOperator contextLogicalOperator;
  protected WildcardOperator contextWildcardOperator;
  protected DataGraphConfig graph;
  protected TableConfig table;
  protected ScanLiterals partialKeyScanLiterals = new ScanLiterals();
  protected ScanLiterals fuzzyKeyScanLiterals = new ScanLiterals();
  protected ScanLiteralFactory scanLiteralFactory = new ScanLiteralFactory();

  @SuppressWarnings("unused")
  private ScanLiteralAssembler() {
  }

  public ScanLiteralAssembler(PlasmaType rootType) {
    this.rootType = rootType;
    this.contextType = this.rootType;
    QName rootTypeQname = this.rootType.getQualifiedName();
    this.graph = CloudGraphConfig.getInstance().getDataGraph(rootTypeQname);
    this.table = CloudGraphConfig.getInstance().getTable(rootTypeQname);
  }

  public ScanLiterals getPartialKeyScanResult() {
    return this.partialKeyScanLiterals;
  }

  public ScanLiterals getFuzzyKeyScanResult() {
    return this.fuzzyKeyScanLiterals;
  }

  /**
   * Assemble the set of data "flavor" and data type specific scan literals used
   * to construct composite partial row (start/stop) key pair.
   * 
   * @param where
   *          the row predicate hierarchy
   * @param contextType
   *          the context type which may be the root type or another type linked
   *          by one or more relations to the root
   */
  public void assemble(Where where, PlasmaType contextType) {

    this.contextType = contextType;

    if (log.isDebugEnabled())
      log.debug("begin traverse");

    where.accept(this); // traverse

    if (log.isDebugEnabled())
      log.debug("end traverse");
  }

  /**
   * Process the traversal start event for a query
   * {@link org.plasma.query.model.Property property} within an
   * {@link org.plasma.query.model.Expression expression} just traversing the
   * property path if exists and capturing context information for the current
   * {@link org.plasma.query.model.Expression expression}.
   * 
   * @see org.plasma.query.visitor.DefaultQueryVisitor#start(org.plasma.query.model.Property)
   */
  @Override
  public void start(Property property) {
    Path path = property.getPath();
    PlasmaType targetType = (PlasmaType) this.rootType;
    if (path != null) {
      for (int i = 0; i < path.getPathNodes().size(); i++) {
        AbstractPathElement pathElem = path.getPathNodes().get(i).getPathElement();
        if (pathElem instanceof WildcardPathElement)
          throw new GraphServiceException(
              "wildcard path elements applicable for 'Select' clause paths only, not 'Where' clause paths");
        String elem = ((PathElement) pathElem).getValue();
        PlasmaProperty prop = (PlasmaProperty) targetType.getProperty(elem);
        targetType = (PlasmaType) prop.getType(); // traverse
      }
    }
    PlasmaProperty endpointProp = (PlasmaProperty) targetType.getProperty(property.getName());
    this.contextProperty = endpointProp;
    this.contextType = targetType;
    this.contextPropertyPath = property.asPathString();

    super.start(property);
  }

  /**
   * Process the traversal start event for a query
   * {@link org.plasma.query.model.Literal literal} within an
   * {@link org.plasma.query.model.Expression expression}.
   * 
   * @param literal
   *          the expression literal
   * @throws GraphServiceException
   *           if no user defined row-key token is configured for the current
   *           literal context.
   */
  @Override
  public void start(Literal literal) {
    String content = literal.getValue();
    if (this.contextProperty == null)
      throw new IllegalStateException("expected context property for literal");
    if (this.contextType == null)
      throw new IllegalStateException("expected context type for literal");
    if (this.rootType == null)
      throw new IllegalStateException("expected context type for literal");

    // Match the current property to a user defined
    // row key token, if found we can process
    UserDefinedRowKeyFieldConfig fieldConfig = this.graph
        .getUserDefinedRowKeyField(this.contextPropertyPath);
    if (fieldConfig != null) {
      PlasmaProperty property = (PlasmaProperty) fieldConfig.getEndpointProperty();
      ScanLiteral scanLiteral = null;
      if (this.contextRelationalOperator != null) {
        scanLiteral = this.scanLiteralFactory.createLiteral(content, property, this.rootType,
            this.contextRelationalOperator, fieldConfig);
        // partial scan does not accommodate 'not equals' as it scans
        // for
        // contiguous set of row keys
        if (this.contextRelationalOperator.getValue().ordinal() != RelationalOperatorValues.NOT_EQUALS
            .ordinal())
          this.partialKeyScanLiterals.addLiteral(scanLiteral);
        // fuzzy only does 'equals' and wildcards
        if (this.contextRelationalOperator.getValue().ordinal() == RelationalOperatorValues.EQUALS
            .ordinal())
          this.fuzzyKeyScanLiterals.addLiteral(scanLiteral);
      } else if (this.contextWildcardOperator != null) {
        scanLiteral = this.scanLiteralFactory.createLiteral(content, property, this.rootType,
            this.contextWildcardOperator, fieldConfig);
        this.fuzzyKeyScanLiterals.addLiteral(scanLiteral);
      } else
        throw new GraphServiceException("expected relational or wildcard operator for query path '"
            + this.contextPropertyPath + "'");
    } else
      log.warn("no user defined row-key field for query path '" + this.contextPropertyPath
          + "' - deferring to graph recogniser post processor");

    super.start(literal);
  }

  /**
   * (non-Javadoc)
   * 
   * @see org.plasma.query.visitor.DefaultQueryVisitor#start(org.plasma.query.model.NullLiteral)
   */
  @Override
  public void start(NullLiteral nullLiteral) {
    throw new GraphServiceException("null literals for row scans not yet supported");
  }

  /**
   * Process a {@link org.plasma.query.model.LogicalOperator logical operator}
   * query traversal start event.
   */
  public void start(LogicalOperator operator) {

    switch (operator.getValue()) {
    case AND:
    case OR:
      this.contextLogicalOperator = operator;
    }
    super.start(operator);
  }

  public void start(WildcardOperator operator) {
    switch (operator.getValue()) {
    default:
      this.contextRelationalOperator = null;
      this.contextWildcardOperator = operator;
    }
  }

  @Override
  public void start(RelationalOperator operator) {
    switch (operator.getValue()) {
    case EQUALS:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_EQUALS:
    case LESS_THAN:
    case LESS_THAN_EQUALS:
      this.contextRelationalOperator = operator;
      this.contextWildcardOperator = null;
      break;
    default:
      throw new GraphServiceException("unknown relational operator '"
          + operator.getValue().toString() + "'");
    }
    super.start(operator);
  }

  public void start(GroupOperator operator) {
    switch (operator.getValue()) {
    case RP_1:
      break;
    case RP_2:
      break;
    case RP_3:
      break;
    case LP_1:
      break;
    case LP_2:
      break;
    case LP_3:
      break;
    default:
      throw new GraphServiceException("unsupported group operator, " + operator.getValue().name());
    }
    super.start(operator);
  }

}
