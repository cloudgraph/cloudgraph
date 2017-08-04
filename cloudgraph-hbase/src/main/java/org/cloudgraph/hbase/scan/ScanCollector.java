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
package org.cloudgraph.hbase.scan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.DataGraphConfig;
import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprVisitor;
import org.cloudgraph.query.expr.LogicalBinaryExpr;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.cloudgraph.query.expr.WildcardBinaryExpr;
import org.plasma.query.model.LogicalOperatorValues;
import org.plasma.query.model.RelationalOperatorValues;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Collector visitor which supports the "recognition" of one or more
 * {@link PartialRowKey partial}, {@link FuzzyRowKey fuzzy} and other scan
 * constructs within the context of a binary (query) {@link Expr expression}
 * syntax tree, encapsulating operator precedence and other factors.
 * <p>
 * Composite row key scans represent only
 * {@link org.cloudgraph.hbase.expr.LogicalBinaryExpr logical binary} 'AND'
 * expressions across the key fields. So for
 * {@link org.cloudgraph.hbase.expr.RelationalBinaryExpr relational binary}
 * expressions linked within a query syntax tree by one or more logical binary
 * 'AND', expressions, a single {@link PartialRowKey partial} or
 * {@link FuzzyRowKey fuzzy} row key scan may be used. But for
 * {@link org.cloudgraph.hbase.expr.RelationalBinaryExpr relational binary}
 * expressions linked by {@link org.cloudgraph.hbase.expr.LogicalBinaryExpr
 * logical binary} 'OR' expressions multiple scans must be used. Clients of this
 * collector class may execute the resulting scans in series or in parallel
 * depending on various performance and other considerations.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see org.cloudgraph.hbase.expr.Expr
 * @see org.cloudgraph.hbase.expr.BinaryExpr
 * @see org.cloudgraph.hbase.expr.ExprVisitor
 * @see org.cloudgraph.config.DataGraphConfig
 * @see org.cloudgraph.hbase.expr.LogicalBinaryExpr
 * @see org.cloudgraph.hbase.expr.RelationalBinaryExpr
 * @see org.cloudgraph.hbase.expr.WildcardBinaryExpr
 */
public class ScanCollector implements ExprVisitor {

  private static Log log = LogFactory.getLog(ScanCollector.class);
  private List<Map<UserDefinedRowKeyFieldConfig, List<ScanLiteral>>> literals = new ArrayList<Map<UserDefinedRowKeyFieldConfig, List<ScanLiteral>>>();

  private PlasmaType rootType;
  private DataGraphConfig graph;
  private List<PartialRowKey> partialKeyScans;
  private List<FuzzyRowKey> fuzzyKeyScans;
  private List<CompleteRowKey> completeKeys;
  private ScanLiteralFactory factory = new ScanLiteralFactory();
  private boolean queryRequiresGraphRecognizer = false;

  public ScanCollector(PlasmaType rootType) {
    this.rootType = rootType;
    QName rootTypeQname = this.rootType.getQualifiedName();
    this.graph = CloudGraphConfig.getInstance().getDataGraph(rootTypeQname);
  }

  private void init() {
    if (this.partialKeyScans == null) {
      this.partialKeyScans = new ArrayList<PartialRowKey>(this.literals.size());
      this.fuzzyKeyScans = new ArrayList<FuzzyRowKey>(this.literals.size());
      this.completeKeys = new ArrayList<CompleteRowKey>(this.literals.size());
      for (Map<UserDefinedRowKeyFieldConfig, List<ScanLiteral>> existing : this.literals) {
        ScanLiterals scanLiterals = new ScanLiterals();
        for (List<ScanLiteral> literalList : existing.values()) {
          for (ScanLiteral literal : literalList)
            scanLiterals.addLiteral(literal);
        }

        // gives precedence to complete keys over partial keys and to
        // partial
        // keys over fuzzy keys

        if (scanLiterals.supportCompleteRowKey(this.graph)) {
          CompleteRowKeyAssembler assembler = new CompleteRowKeyAssembler(this.rootType);
          assembler.assemble(scanLiterals);
          this.completeKeys.add(assembler);
        } else if (scanLiterals.supportPartialRowKeyScan(this.graph)) {
          PartialRowKeyScanAssembler assembler = new PartialRowKeyScanAssembler(this.rootType);
          assembler.assemble(scanLiterals);
          this.partialKeyScans.add(assembler);
        } else {
          FuzzyRowKeyScanAssembler assembler = new FuzzyRowKeyScanAssembler(this.rootType);
          assembler.assemble(scanLiterals);
          this.fuzzyKeyScans.add(assembler);
        }
      }
    }
  }

  public boolean isQueryRequiresGraphRecognizer() {
    return queryRequiresGraphRecognizer;
  }

  public List<PartialRowKey> getPartialRowKeyScans() {
    init();
    return this.partialKeyScans;
  }

  public List<FuzzyRowKey> getFuzzyRowKeyScans() {
    init();
    return this.fuzzyKeyScans;
  }

  public List<CompleteRowKey> getCompleteRowKeys() {
    init();
    return this.completeKeys;
  }

  @Override
  public void visit(Expr target, Expr source, int level) {
    if (target instanceof RelationalBinaryExpr) {
      RelationalBinaryExpr expr = (RelationalBinaryExpr) target;
      collect(expr, source);
    } else if (target instanceof WildcardBinaryExpr) {
      WildcardBinaryExpr expr = (WildcardBinaryExpr) target;
      collect(expr, source);
    }
  }

  private void collect(RelationalBinaryExpr target, Expr source) {
    UserDefinedRowKeyFieldConfig fieldConfig = graph.getUserDefinedRowKeyField(target
        .getPropertyPath());
    if (fieldConfig == null) {
      log.warn("no user defined row-key field for query path '" + target.getPropertyPath()
          + "' - deferring to graph recogniser post processor");
      this.queryRequiresGraphRecognizer = true;
      return;
    }
    PlasmaProperty property = (PlasmaProperty) fieldConfig.getEndpointProperty();

    ScanLiteral scanLiteral = factory.createLiteral(target.getLiteral().getValue(), property,
        (PlasmaType) graph.getRootType(), target.getOperator(), fieldConfig);
    if (log.isDebugEnabled())
      log.debug("collecting path: " + target.getPropertyPath());
    collect(scanLiteral, fieldConfig, source);
  }

  private void collect(WildcardBinaryExpr target, Expr source) {
    UserDefinedRowKeyFieldConfig fieldConfig = graph.getUserDefinedRowKeyField(target
        .getPropertyPath());
    if (fieldConfig == null) {
      log.warn("no user defined row-key field for query path '" + target.getPropertyPath()
          + "' - deferring to graph recogniser post processor");
      this.queryRequiresGraphRecognizer = true;
      return;
    }
    PlasmaProperty property = (PlasmaProperty) fieldConfig.getEndpointProperty();

    ScanLiteral scanLiteral = factory.createLiteral(target.getLiteral().getValue(), property,
        (PlasmaType) graph.getRootType(), target.getOperator(), fieldConfig);
    if (log.isDebugEnabled())
      log.debug("collecting path: " + target.getPropertyPath());
    collect(scanLiteral, fieldConfig, source);
  }

  private void collect(ScanLiteral scanLiteral, UserDefinedRowKeyFieldConfig fieldConfig,
      Expr source) {
    if (source != null) {
      if (source instanceof LogicalBinaryExpr) {
        LogicalBinaryExpr lbe = (LogicalBinaryExpr) source;
        this.collect(fieldConfig, lbe, scanLiteral);
      } else
        throw new IllegalOperatorMappingException("expected logical binary expression parent not, "
            + source.getClass().getName());
    } else {
      this.collect(fieldConfig, null, scanLiteral);
    }
  }

  private void collect(UserDefinedRowKeyFieldConfig fieldConfig, LogicalBinaryExpr source,
      ScanLiteral scanLiteral) {
    if (this.literals.size() == 0) {
      Map<UserDefinedRowKeyFieldConfig, List<ScanLiteral>> map = new HashMap<UserDefinedRowKeyFieldConfig, List<ScanLiteral>>();
      List<ScanLiteral> list = new ArrayList<ScanLiteral>(2);
      list.add(scanLiteral);
      map.put(fieldConfig, list);
      this.literals.add(map);
    } else if (this.literals.size() > 0) {
      boolean foundField = false;

      for (Map<UserDefinedRowKeyFieldConfig, List<ScanLiteral>> existingMap : literals) {
        if (source == null
            || source.getOperator().getValue().ordinal() == LogicalOperatorValues.AND.ordinal()) {
          List<ScanLiteral> list = existingMap.get(fieldConfig);
          if (list == null) {
            list = new ArrayList<ScanLiteral>();
            list.add(scanLiteral);
            existingMap.put(fieldConfig, list);
          } else if (list.size() == 1) {
            ScanLiteral existingLiteral = list.get(0);
            // FIXME: 2 and-ed wildcard expressions cause a NPE here
            // as there is no relational operator in a WC
            RelationalOperatorValues existingOperator = existingLiteral.getRelationalOperator()
                .getValue();
            switch (scanLiteral.getRelationalOperator().getValue()) {
            case GREATER_THAN:
            case GREATER_THAN_EQUALS:
              if (existingOperator.ordinal() != RelationalOperatorValues.LESS_THAN.ordinal()
                  && existingOperator.ordinal() != RelationalOperatorValues.LESS_THAN_EQUALS
                      .ordinal())
                throw new ImbalancedOperatorMappingException(scanLiteral.getRelationalOperator()
                    .getValue(), LogicalOperatorValues.AND, existingOperator, fieldConfig);
              list.add(scanLiteral);
              break;
            case LESS_THAN:
            case LESS_THAN_EQUALS:
              if (existingOperator.ordinal() != RelationalOperatorValues.GREATER_THAN.ordinal()
                  && existingOperator.ordinal() != RelationalOperatorValues.GREATER_THAN_EQUALS
                      .ordinal())
                throw new ImbalancedOperatorMappingException(scanLiteral.getRelationalOperator()
                    .getValue(), LogicalOperatorValues.AND, existingOperator, fieldConfig);
              list.add(scanLiteral);
              break;
            case EQUALS:
            case NOT_EQUALS:
            default:
              throw new IllegalOperatorMappingException("relational operator '"
                  + scanLiteral.getRelationalOperator().getValue()
                  + "' linked through logical operator '" + LogicalOperatorValues.AND
                  + "to row key field property, "
                  + fieldConfig.getEndpointProperty().getContainingType().toString() + "."
                  + fieldConfig.getEndpointProperty().getName());
            }
          } else {
            throw new IllegalOperatorMappingException("logical operator '"
                + LogicalOperatorValues.AND + "' mapped more than 2 times "
                + "to row key field property, "
                + fieldConfig.getEndpointProperty().getContainingType().toString() + "."
                + fieldConfig.getEndpointProperty().getName());
          }
        } else if (source.getOperator().getValue().ordinal() == LogicalOperatorValues.OR.ordinal()) {
          List<ScanLiteral> list = existingMap.get(fieldConfig);
          if (list == null) {
            if (foundField)
              throw new IllegalStateException("expected for key field mapped to scans "
                  + "for row key field property, "
                  + fieldConfig.getEndpointProperty().getContainingType().toString() + "."
                  + fieldConfig.getEndpointProperty().getName());
            list = new ArrayList<ScanLiteral>();
            list.add(scanLiteral);
            existingMap.put(fieldConfig, list);
          } else {
            foundField = true;
          }
        } else {
          log.warn("unsuported logical operator, " + source.getOperator().getValue()
              + " - ignoring");
        }
      }

      if (foundField) {
        // duplicate any map with new literal
        Map<UserDefinedRowKeyFieldConfig, List<ScanLiteral>> next = newMap(literals.get(0),
            fieldConfig, scanLiteral);
        literals.add(next);
      }
    }
  }

  /**
   * Duplicates the given map except with the given literal replacing the
   * mapping for the given field configuration
   * 
   * @param existing
   *          the existing source map
   * @param fieldConfig
   *          the fiend configuration
   * @param scanLiteral
   *          the literal
   * @return the new map
   */
  private Map<UserDefinedRowKeyFieldConfig, List<ScanLiteral>> newMap(
      Map<UserDefinedRowKeyFieldConfig, List<ScanLiteral>> existing,
      UserDefinedRowKeyFieldConfig fieldConfig, ScanLiteral scanLiteral) {
    Map<UserDefinedRowKeyFieldConfig, List<ScanLiteral>> next = new HashMap<UserDefinedRowKeyFieldConfig, List<ScanLiteral>>();
    for (UserDefinedRowKeyFieldConfig config : existing.keySet()) {
      if (!config.equals(fieldConfig)) {
        next.put(config, existing.get(config));
      } else {
        List<ScanLiteral> list = new ArrayList<ScanLiteral>(2);
        list.add(scanLiteral);
        next.put(fieldConfig, list);
      }
    }
    return next;
  }

}
