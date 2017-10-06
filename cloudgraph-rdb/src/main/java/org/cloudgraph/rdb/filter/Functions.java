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

import java.util.List;

import org.plasma.query.model.Function;
import org.plasma.runtime.DataAccessProviderName;
import org.plasma.runtime.PlasmaRuntime;
import org.plasma.runtime.RDBMSVendorName;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.access.DataAccessException;

public class Functions {

  public static String wrap(PlasmaProperty endpointProp, List<Function> functions,
      String targetAlias) {
    DataFlavor flavor = endpointProp.getDataFlavor();
    RDBMSVendorName vendor = PlasmaRuntime.getInstance().getRDBMSProviderVendor(
        DataAccessProviderName.JDBC);
    StringBuilder buf = new StringBuilder();

    // for each function feed the results into the next
    String result = targetAlias + "." + endpointProp.getPhysicalName();
    for (Function func : functions) {
      validateFunction(func, endpointProp, flavor);
      switch (vendor) {
      case ORACLE:
        result = beginFunctionOracle(func, endpointProp, result);
        break;
      case MYSQL:
        result = beginFunctionMySql(func, endpointProp, result);
        break;
      default:
      }
    }

    return result;
  }

  private static String beginFunctionOracle(Function func, PlasmaProperty endpointProp,
      String propertyArg) {
    StringBuilder buf = new StringBuilder();

    switch (func.getName()) {
    case MIN:
    case MAX:
    case AVG:
    case SUM:
      break; // handled entirely above
    case ABS:
      buf.append("ABS(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case CEILING:
      buf.append("CEIL(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case FLOOR:
      buf.append("FLOOR(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case ROUND:
      buf.append("ROUND(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case SUBSTRING_BEFORE:
      String arg = func.getFunctionArgs().get(0).getValue(); // checked
      // above
      buf.append("SUBSTR(");
      buf.append(propertyArg);
      buf.append(",0, INSTR(");
      buf.append(propertyArg);
      buf.append(",'");
      buf.append(arg);
      buf.append("'))");
      break;
    case SUBSTRING_AFTER:
      arg = func.getFunctionArgs().get(0).getValue(); // checked above
      buf.append("SUBSTR(");
      buf.append(propertyArg);
      buf.append(",INSTR(");
      buf.append(propertyArg);
      buf.append(",'");
      buf.append(arg);
      buf.append("'))");
      break;
    case NORMALIZE_SPACE: // trims leading and trailing whitespace and
      // replaces internal whitespace sequences
      // with a since space
      buf.append("REGEXP_REPLACE(TRIM(");
      buf.append(propertyArg);
      buf.append("), '\\s+', ' ')");
      break;
    case UPPER_CASE:
      buf.append("UPPER(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case LOWER_CASE:
      buf.append("LOWER(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case YEAR_FROM_DATE:
    case MONTH_FROM_DATE:
    case DAY_FROM_DATE:
    }

    return buf.toString();
  }

  private static String beginFunctionMySql(Function func, PlasmaProperty endpointProp,
      String propertyArg) {
    StringBuilder buf = new StringBuilder();
    switch (func.getName()) {
    case MIN:
    case MAX:
    case AVG:
    case SUM:
      break; // handled entirely above
    case ABS:
      buf.append("ABS(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case CEILING:
      buf.append("CEIL(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case FLOOR:
      buf.append("FLOOR(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case ROUND:
      buf.append("ROUND(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case SUBSTRING_BEFORE:
      String arg = func.getFunctionArgs().get(0).getValue(); // checked
      // above
      buf.append("SUBSTRING_INDEX(");
      buf.append(propertyArg);
      buf.append(",'");
      buf.append(arg);
      buf.append("', 1)");
      break;
    case SUBSTRING_AFTER:
      arg = func.getFunctionArgs().get(0).getValue(); // checked above
      buf.append("SUBSTR(");
      buf.append(propertyArg);
      buf.append(",LOCATE(");
      buf.append("'");
      buf.append(arg);
      buf.append("'");
      buf.append(",");
      buf.append(propertyArg);
      buf.append("))");
      break;
    case NORMALIZE_SPACE:
      buf.append("TRIM(BOTH ' ' FROM ");
      buf.append(propertyArg);
      buf.append(")");
      // /buf.append("REPLACE(");
      // /buf.append(propertyArg);
      // /buf.append(", ' ', '')"); // note will not take a regexp
      break;
    case UPPER_CASE:
      buf.append("UPPER(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case LOWER_CASE:
      buf.append("LOWER(");
      buf.append(propertyArg);
      buf.append(")");
      break;
    case YEAR_FROM_DATE:
    case MONTH_FROM_DATE:
    case DAY_FROM_DATE:
    }
    return buf.toString();
  }

  /**
   * Performs pre checks on the given function ensuring the correct datatype and
   * number of arguments, etc..
   * 
   * @param func
   *          the function
   * @param endpointProp
   *          the property
   * @param flavor
   *          the property data flavor
   */
  private static void validateFunction(Function func, PlasmaProperty endpointProp, DataFlavor flavor) {

    switch (func.getName()) {
    case MIN:
    case MAX:
    case AVG:
    case SUM:
      throw new DataAccessException("aggregate function '" + func.getName()
          + "' not applicable in query Where clause");
    case ABS:
    case CEILING:
    case FLOOR:
    case ROUND:
      if (!isNumber(flavor))
        throw new DataAccessException("function '" + func.getName()
            + "' not applicable for non numeric property, " + endpointProp.toString());
      break;
    case SUBSTRING_BEFORE:
    case SUBSTRING_AFTER:
      if (func.getFunctionArgs().size() != 1) {
        throw new DataAccessException("function '" + func.getName()
            + "' requires a single string argument - " + func.getFunctionArgs().size()
            + " argument(s) found");
      }
      break;
    case NORMALIZE_SPACE:
    case UPPER_CASE:
    case LOWER_CASE:
      if (!isString(flavor))
        throw new DataAccessException("function '" + func.getName()
            + "' not applicable for non-string property, " + endpointProp.toString());
      break;
    case YEAR_FROM_DATE:
    case MONTH_FROM_DATE:
    case DAY_FROM_DATE:
      if (!isString(flavor))
        throw new DataAccessException("function '" + func.getName()
            + "' not applicable for non-temporal property, " + endpointProp.toString());
      break;
    }
  }

  private static boolean isNumber(DataFlavor flavor) {
    switch (flavor) {
    case integral:
    case real:
      return true;
    default:
    }
    return false;
  }

  private static boolean isString(DataFlavor flavor) {
    switch (flavor) {
    case string:
      return true;
    default:
    }
    return false;
  }
}
