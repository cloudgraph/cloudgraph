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
import org.cloudgraph.query.expr.Expr;
import org.cloudgraph.query.expr.ExprVisitor;
import org.cloudgraph.query.expr.LogicalBinaryExpr;
import org.cloudgraph.query.expr.PredicateBinaryExpr;
import org.cloudgraph.query.expr.RelationalBinaryExpr;
import org.cloudgraph.store.mapping.DataGraphMapping;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.model.LogicalOperatorName;
import org.plasma.query.model.RelationalOperatorName;
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
 * @see org.cloudgraph.store.mapping.DataGraphMapping
 * @see org.cloudgraph.hbase.expr.LogicalBinaryExpr
 * @see org.cloudgraph.hbase.expr.RelationalBinaryExpr
 * @see org.cloudgraph.hbase.expr.PredicateBinaryExpr
 */
public class ScanCollector implements ExprVisitor {

  private static Log log = LogFactory.getLog(ScanCollector.class);
  private List<Map<DataRowKeyFieldMapping, List<ScanLiteral>>> literals = new ArrayList<Map<DataRowKeyFieldMapping, List<ScanLiteral>>>();

  private PlasmaType rootType;
  private StoreMappingContext mappingContext;
  private DataGraphMapping graph;
  private List<PartialRowKey> partialKeyScans;
  private List<FuzzyRowKey> fuzzyKeyScans;
  private List<CompleteRowKey> completeKeys;
  private ScanLiteralFactory factory = new ScanLiteralFactory();
  private boolean queryRequiresGraphRecognizer = false;

  public ScanCollector(PlasmaType rootType, StoreMappingContext mappingContext) {
    this.rootType = rootType;
    this.mappingContext = mappingContext;
    QName rootTypeQname = this.rootType.getQualifiedName();
    this.graph = StoreMapping.getInstance().getDataGraph(rootTypeQname, this.mappingContext);
  }

  private void init() {
    if (this.partialKeyScans == null) {
      this.partialKeyScans = new ArrayList<PartialRowKey>(this.literals.size());
      this.fuzzyKeyScans = new ArrayList<FuzzyRowKey>(this.literals.size());
      this.completeKeys = new ArrayList<CompleteRowKey>(this.literals.size());
      for (Map<DataRowKeyFieldMapping, List<ScanLiteral>> existing : this.literals) {
        ScanLiterals scanLiterals = new ScanLiterals();
        for (List<ScanLiteral> literalList : existing.values()) {
          for (ScanLiteral literal : literalList)
            scanLiterals.addLiteral(literal);
        }

        // gives precedence to complete keys over partial keys and to
        // partial
        // keys over fuzzy keys

        if (scanLiterals.supportCompleteRowKey(this.graph)) {
          CompleteRowKeyAssembler assembler = new CompleteRowKeyAssembler(this.rootType,
              this.mappingContext);
          assembler.assemble(scanLiterals);
          this.completeKeys.add(assembler);
        } else if (scanLiterals.supportPartialRowKeyScan(this.graph)) {
          PartialRowKeyScanAssembler assembler = new PartialRowKeyScanAssembler(this.rootType,
              this.mappingContext);
          assembler.assemble(scanLiterals);
          this.partialKeyScans.add(assembler);
        } else {
          FuzzyRowKeyScanAssembler assembler = new FuzzyRowKeyScanAssembler(this.rootType,
              this.mappingContext);
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
    } else if (target instanceof PredicateBinaryExpr) {
      PredicateBinaryExpr expr = (PredicateBinaryExpr) target;
      collect(expr, source);
    }
  }

  private void collect(RelationalBinaryExpr target, Expr source) {
    LogicalOperatorName logicalOperContext = null;
    if (source != null && LogicalBinaryExpr.class.isInstance(source)) {
      LogicalBinaryExpr lbe = LogicalBinaryExpr.class.cast(source);
      logicalOperContext = lbe.getOperator().getValue();
    }
    DataRowKeyFieldMapping fieldConfig = graph.getUserDefinedRowKeyField(target.getPropertyPath());
    if (fieldConfig == null) {
      log.warn("no user defined row-key field for query path '" + target.getPropertyPath()
          + "' - deferring to graph recogniser post processor");
      this.queryRequiresGraphRecognizer = true;
      return;
    }
    PlasmaProperty property = (PlasmaProperty) fieldConfig.getEndpointProperty();

    ScanLiteral scanLiteral = factory.createLiteral(target.getLiteral().getValue(), property,
        (PlasmaType) graph.getRootType(), target.getOperator(), logicalOperContext, fieldConfig,
        this.mappingContext);
    if (log.isDebugEnabled())
      log.debug("collecting path: " + target.getPropertyPath());
    collect(scanLiteral, fieldConfig, source);
  }

  private void collect(PredicateBinaryExpr target, Expr source) {
    LogicalOperatorName logicalOperContext = null;
    if (source != null && LogicalBinaryExpr.class.isInstance(source)) {
      LogicalBinaryExpr lbe = LogicalBinaryExpr.class.cast(source);
      logicalOperContext = lbe.getOperator().getValue();
    }
    DataRowKeyFieldMapping fieldConfig = graph.getUserDefinedRowKeyField(target.getPropertyPath());
    if (fieldConfig == null) {
      log.warn("no user defined row-key field for query path '" + target.getPropertyPath()
          + "' - deferring to graph recogniser post processor");
      this.queryRequiresGraphRecognizer = true;
      return;
    }
    PlasmaProperty property = (PlasmaProperty) fieldConfig.getEndpointProperty();
    switch (target.getOperator().getValue()) {
    case IN:
      String[] literals = new String[0];
      if (target.getLiteral() != null) {
        if (target.getLiteral().getDelimiter() != null) {
          literals = target.getLiteral().getValue().split(target.getLiteral().getDelimiter());
        } else {
          log.warn("no delimiter found for literal value '" + target.getLiteral().getValue()
              + "' - using space char");
          literals = target.getLiteral().getValue().split(" ");
        }
      }
      for (String literal : literals) {
        ScanLiteral scanLiteral = factory.createLiteral(literal, property,
            (PlasmaType) graph.getRootType(), target.getOperator(), LogicalOperatorName.OR,
            fieldConfig, this.mappingContext);
        this.collect(fieldConfig, LogicalOperatorName.OR, scanLiteral);
      }
      break;
    default:
      ScanLiteral scanLiteral = factory.createLiteral(target.getLiteral().getValue(), property,
          (PlasmaType) graph.getRootType(), target.getOperator(), logicalOperContext, fieldConfig,
          this.mappingContext);
      if (log.isDebugEnabled())
        log.debug("collecting path: " + target.getPropertyPath());
      collect(scanLiteral, fieldConfig, source);
    }

  }

  private void collect(ScanLiteral scanLiteral, DataRowKeyFieldMapping fieldConfig, Expr source) {
    if (source != null) {
      if (source instanceof LogicalBinaryExpr) {
        LogicalBinaryExpr lbe = (LogicalBinaryExpr) source;
        this.collect(fieldConfig, lbe.getOperator().getValue(), scanLiteral);
      } else
        throw new IllegalOperatorMappingException("expected logical binary expression parent not, "
            + source.getClass().getName());
    } else {
      this.collect(fieldConfig, null, scanLiteral);
    }
  }

  private void collect(DataRowKeyFieldMapping fieldConfig,
      LogicalOperatorName logicalOperatorContext, ScanLiteral scanLiteral) {
    if (this.literals.size() == 0) {
      Map<DataRowKeyFieldMapping, List<ScanLiteral>> map = new HashMap<DataRowKeyFieldMapping, List<ScanLiteral>>();
      List<ScanLiteral> list = new ArrayList<ScanLiteral>(2);
      list.add(scanLiteral);
      map.put(fieldConfig, list);
      this.literals.add(map);
    } else if (this.literals.size() > 0) {
      boolean foundField = false;

      for (Map<DataRowKeyFieldMapping, List<ScanLiteral>> existingMap : literals) {
        if (logicalOperatorContext == null
            || logicalOperatorContext.ordinal() == LogicalOperatorName.AND.ordinal()) {
          List<ScanLiteral> list = existingMap.get(fieldConfig);
          if (list == null) {
            list = new ArrayList<ScanLiteral>();
            list.add(scanLiteral);
            existingMap.put(fieldConfig, list);
          } else if (list.size() == 1) {
            ScanLiteral existingLiteral = list.get(0);
            // FIXME: 2 and-ed wildcard expressions cause a NPE here
            // as there is no relational operator in a WC
            RelationalOperatorName existingOperator = existingLiteral.getRelationalOperator();
            switch (scanLiteral.getRelationalOperator()) {
            case GREATER_THAN:
            case GREATER_THAN_EQUALS:
              if (existingOperator.ordinal() != RelationalOperatorName.LESS_THAN.ordinal()
                  && existingOperator.ordinal() != RelationalOperatorName.LESS_THAN_EQUALS
                      .ordinal())
                throw new ImbalancedOperatorMappingException(scanLiteral.getRelationalOperator(),
                    LogicalOperatorName.AND, existingOperator, fieldConfig);
              list.add(scanLiteral);
              break;
            case LESS_THAN:
            case LESS_THAN_EQUALS:
              if (existingOperator.ordinal() != RelationalOperatorName.GREATER_THAN.ordinal()
                  && existingOperator.ordinal() != RelationalOperatorName.GREATER_THAN_EQUALS
                      .ordinal())
                throw new ImbalancedOperatorMappingException(scanLiteral.getRelationalOperator(),
                    LogicalOperatorName.AND, existingOperator, fieldConfig);
              list.add(scanLiteral);
              break;
            case EQUALS:
            case NOT_EQUALS:
            default:
              throw new IllegalOperatorMappingException("relational operator '"
                  + scanLiteral.getRelationalOperator() + "' linked through logical operator '"
                  + LogicalOperatorName.AND + "to row key field property, "
                  + fieldConfig.getEndpointProperty().getContainingType().toString() + "."
                  + fieldConfig.getEndpointProperty().getName());
            }
          } else {
            throw new IllegalOperatorMappingException("logical operator '"
                + LogicalOperatorName.AND + "' mapped more than 2 times "
                + "to row key field property, "
                + fieldConfig.getEndpointProperty().getContainingType().toString() + "."
                + fieldConfig.getEndpointProperty().getName());
          }
        } else if (logicalOperatorContext.ordinal() == LogicalOperatorName.OR.ordinal()) {
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
          log.warn("unsuported logical operator, " + logicalOperatorContext + " - ignoring");
        }
      }

      if (foundField) {
        // duplicate any map with new literal
        Map<DataRowKeyFieldMapping, List<ScanLiteral>> next = newMap(literals.get(0), fieldConfig,
            scanLiteral);
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
  private Map<DataRowKeyFieldMapping, List<ScanLiteral>> newMap(
      Map<DataRowKeyFieldMapping, List<ScanLiteral>> existing, DataRowKeyFieldMapping fieldConfig,
      ScanLiteral scanLiteral) {
    Map<DataRowKeyFieldMapping, List<ScanLiteral>> next = new HashMap<DataRowKeyFieldMapping, List<ScanLiteral>>();
    for (DataRowKeyFieldMapping config : existing.keySet()) {
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
