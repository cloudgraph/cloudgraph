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
package org.cloudgraph.query.expr;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.lang.GraphFilterException;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.model.AbstractPathElement;
import org.plasma.query.model.Expression;
import org.plasma.query.model.GroupOperator;
import org.plasma.query.model.GroupOperatorName;
import org.plasma.query.model.Literal;
import org.plasma.query.model.LogicalOperator;
import org.plasma.query.model.LogicalOperatorName;
import org.plasma.query.model.Path;
import org.plasma.query.model.PathElement;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperator;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.query.model.Term;
import org.plasma.query.model.Where;
import org.plasma.query.model.PredicateOperator;
import org.plasma.query.model.PredicateOperatorName;
import org.plasma.query.model.WildcardPathElement;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.xml.sax.SAXException;

/**
 * A binary expression tree assembler which constructs an operator precedence
 * map, then {@link org.cloudgraph.query.expr.ExpresionVisitorSupport visits}
 * (traverses) the given predicate expression syntax tree depth-first using an
 * adapted shunting-yard algorithm and assembles a resulting binary tree
 * structure. In typical usage scenarios, a single expression tree is assembled
 * once, and then used to evaluate any number of graph edge or other results
 * based on a given context.
 * <p>
 * The adapted shunting-yard algorithm in general uses a stack of operators and
 * operands, and as new binary tree nodes are detected and created they are
 * pushed onto the operand stack based on operator precedence. The resulting
 * binary expression tree reflects the syntax of the underlying query expression
 * including the precedence of its operators.
 * </p>
 * <p>
 * The use of binary expression tree evaluation for post processing of graph
 * edge results is necessary in columnar data stores, as an entity with multiple
 * properties is necessarily persisted across multiple columns. And while these
 * data stores provide many useful column oriented filters, the capability to
 * select an entity based on complex criteria which spans several columns is
 * generally not supported, as such filters are column oriented. Yet even for
 * simple queries (e.g. "where entity.c1 = 'foo' and entity.c2 = 'bar'") column
 * c1 and its value exists in one cell and column c2 exists in another table
 * cell. Since columnar data store filters cannot generally span columns, both
 * cells must be returned and the results post processed within the context of
 * the binary expression tree.
 * </p>
 * <p>
 * Subclasses may provide alternate implementations of {@link ExprAssembler}
 * which create binary expression tree nodes with specific evaluation behavior.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 * 
 * @see Expr
 * 
 */
public abstract class DefaultBinaryExprTreeAssembler extends ExpresionVisitorSupport implements
    ExprAssembler {
  private static Log log = LogFactory.getLog(DefaultBinaryExprTreeAssembler.class);
  private Stack<Operator> operators = new Stack<Operator>();
  private Stack<org.plasma.query.Term> operands = new Stack<org.plasma.query.Term>();
  private Map<Object, Integer> precedenceMap = new HashMap<Object, Integer>();
  private Map<Expression, Expr> exprMap = new HashMap<Expression, Expr>();

  protected Where predicate;
  protected PlasmaType rootType;
  protected PlasmaType contextType;
  protected PlasmaProperty contextProperty;
  protected Property contextQueryProperty;
  protected Expression contextExpression;

  @SuppressWarnings("unused")
  private DefaultBinaryExprTreeAssembler() {
  }

  /**
   * Constructs an assembler based on the given predicate and graph edge type.
   * 
   * @param predicate
   *          the predicate
   * @param edgeType
   *          the graph edge type which is the type for the reference property
   *          within the graph which represents an edge
   * @param rootType
   *          the graph root type
   */
  public DefaultBinaryExprTreeAssembler(Where predicate, PlasmaType rootType) {
    this.rootType = rootType;
    this.predicate = predicate;

    precedenceMap.put(LogicalOperatorName.OR, 0);
    precedenceMap.put(LogicalOperatorName.AND, 1);
    precedenceMap.put(PredicateOperatorName.LIKE, 2);
    precedenceMap.put(RelationalOperatorName.EQUALS, 2);
    precedenceMap.put(RelationalOperatorName.NOT_EQUALS, 2);
    precedenceMap.put(RelationalOperatorName.GREATER_THAN, 2);
    precedenceMap.put(RelationalOperatorName.GREATER_THAN_EQUALS, 2);
    precedenceMap.put(RelationalOperatorName.LESS_THAN, 2);
    precedenceMap.put(RelationalOperatorName.LESS_THAN_EQUALS, 2);
    precedenceMap.put(GroupOperatorName.LP_1, 3);
    precedenceMap.put(GroupOperatorName.RP_1, 4);
  }

  /**
   * Returns the binary expression tree result
   * 
   * @return the binary expression tree result
   */
  public Expr getResult() {
    if (log.isDebugEnabled())
      log.debug("begin traverse");

    this.predicate.accept(this); // traverse

    if (log.isDebugEnabled())
      log.debug("end traverse");

    Expression root = this.predicate.getExpressions().get(0);
    Expr result = this.exprMap.get(root);

    if (log.isDebugEnabled()) {
      BinaryExpr binaryExpr = (BinaryExpr) result;
      ExprPrinter printer = new ExprPrinter();
      binaryExpr.accept(printer);
      log.debug("expr: " + printer.toString());
    }

    return result;
  }

  /**
   * Process the traversal end event for a query
   * {@link org.plasma.query.model.Expression expression} consuming each term in
   * the {@link org.plasma.query.model.Expression expression}.
   * 
   * @see org.plasma.query.visitor.DefaultQueryVisitor#end(org.plasma.query.model.Expression)
   */
  @Override
  public void end(Expression expression) {
    this.contextExpression = expression;
    assemble(expression);
  }

  /**
   * Consumes each term in the given {@link org.plasma.query.model.Expression
   * expression} then assembles any remaining terms, mapping the resulting
   * {@link Expr expression}.
   * 
   * @param expression
   *          the expression
   */
  private void assemble(Expression expression) {

    for (int i = 0; i < expression.getTerms().size(); i++) {
      Term term = expression.getTerms().get(i);
      consume(term);
    } // for

    Expr expr = assemble();
    exprMap.put(expression, expr);
    if (log.isDebugEnabled())
      log.debug("mapped: " + expr);
  }

  /**
   * Processes operators and operands currently staged returning an appropriate
   * expression.
   * 
   * @return the expression
   */
  // FIXME: does not support arithmetic or unary expressions
  private Expr assemble() {
    Expr expr = null;
    if (log.isDebugEnabled())
      log.debug("assembling " + this.operands.size() + " operands " + this.operators.size()
          + " operators");

    if (this.operands.peek() instanceof Literal) {
      Literal literal = (Literal) this.operands.pop();
      Property prop = (Property) this.operands.pop();
      Operator oper = this.operators.pop();
      if (oper.getOperator() instanceof RelationalOperator)
        expr = createRelationalBinaryExpr(prop, literal, (RelationalOperator) oper.getOperator());
      else if (oper.getOperator() instanceof PredicateOperator) {
        expr = createPredicateBinaryExpr(prop, literal, (PredicateOperator) oper.getOperator());
      } else
        throw new IllegalStateException("unknown operator, " + oper.toString());
    } else if (this.operands.peek() instanceof Expr) {
      if (this.operands.size() > 1) {
        Expr right = (Expr) this.operands.pop();
        if (this.operands.peek() instanceof Expr) {
          Expr left = (Expr) this.operands.pop();
          LogicalOperator logicalOper = null;
          if (this.operators.size() == 0) {
            if (left instanceof LogicalBinaryExpr) {
              LogicalBinaryExpr leftLogical = (LogicalBinaryExpr) left;
              logicalOper = leftLogical.getOperator();
            } else
              throw new IllegalStateException("expected logical binary not, "
                  + left.getClass().getName());
          } else {
            Operator oper = this.operators.pop();
            logicalOper = (LogicalOperator) oper.getOperator();
          }
          expr = createLogicalBinaryExpr(left, right, logicalOper);
        } else
          throw new IllegalStateException("unknown operand, " + this.operands.peek().toString());
      } else {
        expr = (Expr) this.operands.pop();
      }
    } else
      throw new IllegalStateException("unknown opearand, " + this.operands.peek());

    if (log.isDebugEnabled())
      log.debug("assembled: " + expr);

    return expr;
  }

  /**
   * Consumes the given term, pushing operators and operands onto their
   * respective stack and assembling binary tree nodes based on operator
   * precedence.
   * 
   * @param term
   *          the term
   */
  private void consume(Term term) {
    if (term.getExpression() != null) {
      Expr expr = this.exprMap.get(term.getExpression());
      push(expr);
    } else if (term.getProperty() != null) {
      push(term.getProperty());
    } else if (term.getLiteral() != null) {
      push(term.getLiteral());
    }
    // assemble a node based on operator precedence
    else if (term.getGroupOperator() != null || term.getLogicalOperator() != null
        || term.getRelationalOperator() != null || term.getPredicateOperator() != null) {
      Operator oper = null;
      if (term.getGroupOperator() != null)
        oper = new Operator(term.getGroupOperator(), this.precedenceMap);
      else if (term.getLogicalOperator() != null)
        oper = new Operator(term.getLogicalOperator(), this.precedenceMap);
      else if (term.getPredicateOperator() != null)
        oper = new Operator(term.getPredicateOperator(), this.precedenceMap);
      else
        oper = new Operator(term.getRelationalOperator(), this.precedenceMap);

      if (this.operators.size() > 0) {
        Operator existing = this.operators.peek();
        if (log.isDebugEnabled())
          log.debug("comparing " + existing + " and " + oper);
        if (existing.compareTo(oper) <= 0) {
          Expr expr = assemble(); // expr complete assemble it
          if (this.operators.size() > 0) {
            Operator remaining = this.operators.peek();
            if (!isGroupPair(remaining, oper)) {
              push(oper);
            } else
              this.operators.pop(); // terminate group
          } else {
            push(oper);
          }
          push(expr);
        } else {
          push(oper);
        }
      } else {
        push(oper);
      }
    } else
      throw new IllegalStateException("unexpected term" + getTermClassName(term));
  }

  private void push(Operator oper) {
    this.operators.push(oper);
    if (log.isDebugEnabled())
      log.debug("pushed " + oper);
  }

  private void push(Expr expr) {
    this.operands.push(expr);
    if (log.isDebugEnabled())
      log.debug("pushed expr: " + expr);
  }

  private void push(Property property) {
    this.operands.push(property);
    if (log.isDebugEnabled())
      log.debug("pushed property: " + property.getClass().getSimpleName());
  }

  private void push(Literal literal) {
    this.operands.push(literal);
    if (log.isDebugEnabled())
      log.debug("pushed literal: " + literal.getClass().getSimpleName());
  }

  private boolean isGroupPair(Operator right, Operator left) {
    if (right.getOperator() instanceof GroupOperator) {
      GroupOperator groupRight = (GroupOperator) right.getOperator();
      if (groupRight.getValue().ordinal() == GroupOperatorName.RP_1.ordinal()) {
        if (left.getOperator() instanceof GroupOperator) {
          GroupOperator groupLeft = (GroupOperator) left.getOperator();
          if (groupLeft.getValue().ordinal() == GroupOperatorName.LP_1.ordinal()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Creates and returns a relational binary expression based on the given terms
   * and <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
   * >relational</a> operator.
   * 
   * @param property
   *          the property term
   * @param literal
   *          the literal term
   * @param operator
   *          the <a href=
   *          "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
   *          >relational</a> operator
   * @return a relational binary expression based on the given terms and <a
   *         href=
   *         "http://docs.plasma-sdo.org/api/org/plasma/query/model/RelationalOperator.html"
   *         >relational</a> operator.
   */
  @Override
  public RelationalBinaryExpr createRelationalBinaryExpr(Property property, Literal literal,
      RelationalOperator operator) {
    return new DefaultRelationalBinaryExpr(property, literal, operator);
  }

  /**
   * Creates and returns a wildcard binary expression based on the given terms
   * and <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/WildcardOperator.html"
   * >wildcard</a> operator.
   * 
   * @param property
   *          the property term
   * @param literal
   *          the literal term
   * @param operator
   *          the <a href=
   *          "http://docs.plasma-sdo.org/api/org/plasma/query/model/WildcardOperator.html"
   *          >wildcard</a> operator
   * @return a wildcard binary expression based on the given terms and <a href=
   *         "http://docs.plasma-sdo.org/api/org/plasma/query/model/WildcardOperator.html"
   *         >wildcard</a> operator.
   */
  @Override
  public PredicateBinaryExpr createPredicateBinaryExpr(Property property, Literal literal,
      PredicateOperator operator) {
    return new DefaultPredicateBinaryExpr(property, literal, operator);
  }

  /**
   * Creates and returns a logical binary expression based on the given terms
   * and <a href=
   * "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
   * >logical</a> operator.
   * 
   * @param property
   *          the property term
   * @param literal
   *          the literal term
   * @param operator
   *          the <a href=
   *          "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
   *          >logical</a> operator
   * @return a wildcard binary expression based on the given terms and <a href=
   *         "http://docs.plasma-sdo.org/api/org/plasma/query/model/LogicalOperator.html"
   *         >logical</a> operator.
   */
  @Override
  public LogicalBinaryExpr createLogicalBinaryExpr(Expr left, Expr right, LogicalOperator operator) {
    return new DefaultLogicalBinaryExpr(left, right, operator);
  }

  /**
   * Process the traversal end event for a query
   * {@link org.plasma.query.model.Property property} within an
   * {@link org.plasma.query.model.Expression expression} setting up context
   * information for the endpoint property and its type, as well as physical
   * column qualifier name bytes which are set into the
   * {@link #contextQueryProperty} physical name bytes. for the current
   * {@link org.plasma.query.model.Expression expression}.
   * 
   * @see org.plasma.query.visitor.DefaultQueryVisitor#end(org.plasma.query.model.Property)
   */
  @Override
  public void end(Property property) {
    Path path = property.getPath();

    PlasmaType targetType = traverse(path, this.rootType);
    PlasmaProperty endpointProp = (PlasmaProperty) targetType.getProperty(property.getName());
    this.contextProperty = endpointProp;
    this.contextType = targetType;
    this.contextQueryProperty = property;

    super.end(property);
  }

  protected PlasmaType traverse(Path path, PlasmaType startType) {
    PlasmaType result = startType;
    if (path != null) {
      for (int i = 0; i < path.getPathNodes().size(); i++) {
        AbstractPathElement pathElem = path.getPathNodes().get(i).getPathElement();
        if (pathElem instanceof WildcardPathElement)
          throw new GraphFilterException(
              "wildcard path elements applicable for 'Select' clause paths only, not 'Where' clause paths");
        String elem = ((PathElement) pathElem).getValue();
        PlasmaProperty prop = (PlasmaProperty) result.getProperty(elem);
        result = (PlasmaType) prop.getType(); // traverse
      }
    }
    return result;
  }

  private String getTermClassName(Term term) {
    if (term.getWildcardProperty() != null)
      return term.getWildcardProperty().getClass().getSimpleName();
    else if (term.getPredicateOperator() != null)
      return term.getPredicateOperator().getClass().getSimpleName();
    else if (term.getEntity() != null)
      return term.getEntity().getClass().getSimpleName();
    else if (term.getExpression() != null)
      return term.getExpression().getClass().getSimpleName();
    else if (term.getVariable() != null)
      return term.getVariable().getClass().getSimpleName();
    else if (term.getNullLiteral() != null)
      return term.getNullLiteral().getClass().getSimpleName();
    else if (term.getLiteral() != null)
      return term.getLiteral().getClass().getSimpleName();
    else if (term.getGroupOperator() != null)
      return term.getGroupOperator().getClass().getSimpleName();
    else if (term.getArithmeticOperator() != null)
      return term.getArithmeticOperator().getClass().getSimpleName();
    else if (term.getRelationalOperator() != null)
      return term.getRelationalOperator().getClass().getSimpleName();
    else if (term.getLogicalOperator() != null)
      return term.getLogicalOperator().getClass().getSimpleName();
    else if (term.getProperty() != null)
      return term.getProperty().getClass().getSimpleName();
    else
      return term.getQuery().getClass().getSimpleName();
  }

  protected void log(Expression expr) {
    log.debug("expr: " + serialize(expr));
  }

  protected String serialize(Expression expr) {
    String xml = "";
    PlasmaQueryDataBinding binding;
    try {
      binding = new PlasmaQueryDataBinding(new DefaultValidationEventHandler());
      xml = binding.marshal(expr);
    } catch (JAXBException e) {
      log.debug(e);
    } catch (SAXException e) {
      log.debug(e);
    }
    return xml;
  }

}
