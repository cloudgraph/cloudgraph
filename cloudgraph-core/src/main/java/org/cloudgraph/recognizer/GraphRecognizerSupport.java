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
package org.cloudgraph.recognizer;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.query.model.AbstractPathElement;
import org.plasma.query.model.Function;
import org.plasma.query.model.FunctionName;
import org.plasma.query.model.Path;
import org.plasma.query.model.PathElement;
import org.plasma.query.model.PredicateOperatorName;
import org.plasma.query.model.Property;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.query.model.WildcardPathElement;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.helper.DataConverter;

import commonj.sdo.DataObject;

/**
 * Delegate for graph recognizer expression classes. This class is not thread
 * safe and should not be shared across multiple expression class instances.
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class GraphRecognizerSupport {
  private static Log log = LogFactory.getLog(GraphRecognizerSupport.class);
  @SuppressWarnings("rawtypes")
  private NumberComparator numberComparator;
  @SuppressWarnings("rawtypes")
  private BooleanComparator booleanComparator;
  private DataConverter dataConverter = DataConverter.INSTANCE;
  /** cached wildcard pattern */
  private Pattern wildcardLiteralPattern;

  /**
   * Collects and returns data values at the endpoint of the given path,
   * traversing objects along the the given traversal path if exists.
   * 
   * @param targetObject
   *          the current target
   * @param property
   *          the query property
   * @param path
   *          the query property path
   * @param pathIndex
   *          the current path element index
   * @param values
   *          the collection of result values
   */
  public void collect(DataObject targetObject, Property property, Path path, int pathIndex,
      List<Object> values) {
    PlasmaType targetType = (PlasmaType) targetObject.getType();
    if (path != null && pathIndex < path.getPathNodes().size()) {
      AbstractPathElement pathElem = path.getPathNodes().get(pathIndex).getPathElement();
      if (pathElem instanceof WildcardPathElement)
        throw new GraphServiceException(
            "wildcard path elements applicable for 'Select' clause paths only, not 'Where' clause paths");
      String elem = ((PathElement) pathElem).getValue();
      PlasmaProperty prop = (PlasmaProperty) targetType.getProperty(elem);
      if (targetObject.isSet(prop)) {
        if (prop.isMany()) {
          @SuppressWarnings("unchecked")
          List<DataObject> list = targetObject.getList(prop);
          for (DataObject next : list)
            collect(next, property, path, pathIndex + 1, values);
        } else {
          DataObject next = targetObject.getDataObject(prop);
          collect(next, property, path, pathIndex + 1, values);
        }
      }
    } else {
      PlasmaProperty endpointProp = (PlasmaProperty) targetType.getProperty(property.getName());
      if (!endpointProp.getType().isDataType())
        throw new GraphServiceException("expected datatype property for, " + endpointProp);
      if (property.getFunctions().size() == 0) {
        if (targetObject.isSet(endpointProp)) {
          if (endpointProp.isMany()) {
            @SuppressWarnings("unchecked")
            List<Object> list = targetObject.getList(endpointProp);
            for (Object value : list)
              values.add(value);
          } else {
            Object value = targetObject.get(endpointProp);
            values.add(value);
          }
        }
      } else {
        if (property.getFunctions().size() > 1)
          log.warn("ignoring all but first scalar function of total "
              + property.getFunctions().size());
        Function func = property.getFunctions().get(0);
        if (endpointProp.isMany()) {
          throw new GraphServiceException("expected datatype property for, " + endpointProp);
        } else {
          PlasmaDataObject target = (PlasmaDataObject) targetObject;
          Object value = target.get(func.getName(), endpointProp);
          if (value != null)
            values.add(value);
        }

      }
    }
  }

  /**
   * Returns the SDO property endpoint for the given query property traversal
   * path
   * 
   * @param property
   *          the query property
   * @param rootType
   *          the graph root type
   * @return the SDO property endpoint
   */
  public Endpoint getEndpoint(Property property, PlasmaType rootType) {
    return new RecognizerEndpoint(property, rootType);
  }

  /**
   * Determines the property datatype and evaluates the given property value
   * against the given literal and relational operator.
   * 
   * @param endpoint
   *          the endpoint
   * @param propertyValue
   *          the property data value
   * @param operator
   *          the relational operator
   * @param literal
   *          the query literal
   * @return whether the data property value evaluates true against given
   *         literal and relational operator
   */
  public boolean evaluate(Endpoint endpoint, Object propertyValue, RelationalOperatorName operator,
      String literal) {
    DataType dataType = DataType.valueOf(endpoint.getProperty().getType().getName());
    boolean result = true;

    switch (endpoint.getProperty().getDataFlavor()) {
    case integral:
    case real:
      if (Number.class.isAssignableFrom(propertyValue.getClass())) {
        Number propertyNumberValue = (Number) propertyValue;
        Number literalNumberValue = null;
        if (!endpoint.hasFunctions()) {
          literalNumberValue = (Number) this.dataConverter.convert(
              endpoint.getProperty().getType(), literal);
        } else {
          Function func = endpoint.getSingleFunction();
          FunctionName funcName = func.getName();
          DataType scalarDataType = funcName.getScalarDatatype(dataType);
          literalNumberValue = (Number) this.dataConverter.fromString(scalarDataType, literal);
        }
        result = evaluate(propertyNumberValue, operator, literalNumberValue);
      } else if (Boolean.class.isAssignableFrom(propertyValue.getClass())) {
        Boolean propertyBooleanValue = (Boolean) propertyValue;
        Boolean literalBooleanValue = (Boolean) this.dataConverter.convert(endpoint.getProperty()
            .getType(), literal);
        result = evaluate(propertyBooleanValue, operator, literalBooleanValue);
      } else
        throw new GraphServiceException("unexpected instanceof "
            + propertyValue.getClass().getName() + " for property, " + endpoint.getProperty()
            + " with data flavor " + endpoint.getProperty().getDataFlavor());
      break;
    case string:
      String propertyStringValue = (String) propertyValue;
      String literalStringValue = (String) this.dataConverter.convert(endpoint.getProperty()
          .getType(), literal);
      result = evaluate(propertyStringValue, operator, literalStringValue);
      break;
    case temporal:
      switch (dataType) {
      case Date:
        Date propertyDateValue = (Date) propertyValue;
        Date literalDateValue = (Date) this.dataConverter.convert(endpoint.getProperty().getType(),
            literal);
        result = evaluate(propertyDateValue, operator, literalDateValue);
        break;
      default:
        propertyStringValue = (String) propertyValue;
        literalStringValue = (String) this.dataConverter.convert(endpoint.getProperty().getType(),
            literal);
        result = evaluate(propertyStringValue, operator, literalStringValue);
        break;
      }
      break;
    case other:
      throw new GraphServiceException("data flavor '" + endpoint.getProperty().getDataFlavor()
          + "' not supported for relational operator '" + operator + "'");
    }
    return result;
  }

  /**
   * Determines the property datatype and evaluates the given property value
   * against the given literal and wildcard operator.
   * 
   * @param endpoint
   *          the endpoint
   * @param propertyValue
   *          the property data value
   * @param operator
   *          the wildcard operator
   * @param literal
   *          the query literal
   * @return whether the data property value evaluates true against given
   *         literal and wildcard operator
   */
  public boolean evaluate(Endpoint endpoint, Object propertyValue, PredicateOperatorName operator,
      String literal) {
    boolean result = true;

    switch (operator) {
    case LIKE:
      switch (endpoint.getProperty().getDataFlavor()) {
      case string:
        String propertyStringValue = (String) propertyValue;
        // as trailing newlines confuse regexp greatly
        propertyStringValue = propertyStringValue.trim();
        String literalStringValue = (String) this.dataConverter.convert(endpoint.getProperty()
            .getType(), literal);
        result = evaluate(propertyStringValue, operator, literalStringValue);
        break;
      case integral:
      case real:
      case temporal:
      case other:
        throw new GraphServiceException("data flavor '" + endpoint.getProperty().getDataFlavor()
            + "' not supported for wildcard operator '" + operator + "'");
      }
      break;
    case IN:
      // evals true if property value equals any or given literals
      String[] literals = null;
      if (literal != null)
        literals = literal.split(" ");
      boolean anySuccess = false;
      for (String lit : literals) {
        if (evaluate(endpoint.getProperty(), propertyValue, lit)) {
          anySuccess = true;
          break;
        }
      }
      result = anySuccess;
      break;
    default:
      throw new GraphServiceException("operator '+operator+' not supported for context");
    }
    return result;
  }

  private boolean evaluate(PlasmaProperty property, Object propertyValue, String literal) {
    boolean result = true;
    switch (property.getDataFlavor()) {
    case string:
      String propertyStringValue = (String) propertyValue;
      propertyStringValue = propertyStringValue.trim();
      String literalStringValue = (String) this.dataConverter.convert(property.getType(), literal);
      result = evaluate(propertyStringValue, RelationalOperatorName.EQUALS, literalStringValue);
      break;
    case integral:
    case real:
      Number propertyNumberValue = (Number) propertyValue;
      Number literalNumberValue = (Number) this.dataConverter.convert(property.getType(), literal);
      result = evaluate(propertyNumberValue, RelationalOperatorName.EQUALS, literalNumberValue);
      break;
    case temporal:
    case other:
    default:
      throw new GraphServiceException(
          "data flavor '+property.getDataFlavor()+' not supported for context");
    }
    return result;
  }

  private boolean evaluate(Date propertyValue, RelationalOperatorName operator, Date literalValue) {
    int comp = propertyValue.compareTo(literalValue);
    return evaluate(operator, comp);
  }

  @SuppressWarnings("rawtypes")
  private boolean evaluate(Number propertyValue, RelationalOperatorName operator,
      Number literalValue) {
    if (this.numberComparator == null)
      this.numberComparator = new NumberComparator();
    @SuppressWarnings("unchecked")
    int comp = this.numberComparator.compare(propertyValue, literalValue);
    return evaluate(operator, comp);
  }

  private boolean evaluate(Boolean propertyValue, RelationalOperatorName operator,
      Boolean literalValue) {
    if (this.booleanComparator == null)
      this.booleanComparator = new BooleanComparator();
    int comp = this.booleanComparator.compare(propertyValue, literalValue);
    return evaluate(operator, comp);
  }

  private boolean evaluate(String propertyValue, RelationalOperatorName operator,
      String literalValue) {
    try {
      int comp = propertyValue.compareTo(literalValue);
      return evaluate(operator, comp);
    } catch (NullPointerException e) {
      throw e;
    }
  }

  private boolean evaluate(String propertyValue, PredicateOperatorName operator, String literalValue) {
    if (this.wildcardLiteralPattern == null) {
      String pattern = wildcardToRegex(literalValue);
      this.wildcardLiteralPattern = Pattern.compile(pattern);
    }
    Matcher matcher = this.wildcardLiteralPattern.matcher(propertyValue);
    return matcher.matches();
  }

  private String wildcardToRegex(String wildcard) {
    StringBuffer s = new StringBuffer(wildcard.length());
    s.append('^');
    for (int i = 0, is = wildcard.length(); i < is; i++) {
      char c = wildcard.charAt(i);
      switch (c) {
      case '*':
        s.append(".*");
        break;
      case '?':
        s.append(".");
        break;
      // escape special regexp-characters
      case '(':
      case ')':
      case '[':
      case ']':
      case '$':
      case '^':
      case '.':
      case '{':
      case '}':
      case '|':
      case '\\':
        s.append("\\");
        s.append(c);
        break;
      default:
        s.append(c);
        break;
      }
    }
    s.append('$');
    return (s.toString());
  }

  private boolean evaluate(RelationalOperatorName operator, int comp) {
    switch (operator) {
    case EQUALS:
      return comp == 0;
    case NOT_EQUALS:
      return comp != 0;
    case GREATER_THAN:
      return comp > 0;
    case GREATER_THAN_EQUALS:
      return comp >= 0;
    case LESS_THAN:
      return comp < 0;
    case LESS_THAN_EQUALS:
      return comp <= 0;
    default:
      throw new GraphServiceException("unknown relational operator, " + operator);
    }
  }

  @SuppressWarnings("rawtypes")
  class NumberComparator<T extends Number & Comparable> implements Comparator<T> {

    @SuppressWarnings("unchecked")
    public int compare(T a, T b) throws ClassCastException {
      return a.compareTo(b);
    }
  }

  class BooleanComparator implements Comparator<Boolean> {

    @Override
    public int compare(Boolean a, Boolean b) {
      return a.compareTo(b);
    }
  }
}
