package org.cloudgraph.aerospike.expr;

import org.plasma.query.model.PredicateOperatorName;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;

import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.RegexFlag;

public class ExprUtil {
  public static PredExp[] createPredExp(String binName, PredicateOperatorName oper,
      DataType literalDatType, String literal) {
    PredExp[] result = null;
    switch (oper) {
    case LIKE:
      result = createRegExpPredicate(binName, oper, literalDatType, literal);
      break;
    case APP_OTHER_NAME:
    case BETWEEN:
    case CONTAINS:
    case DISTINCT:
    case EXISTS:
    case IN:
    case MATCH:
    case NOT_EXISTS:
    case NOT_IN:
    case NULL:
    case SIMILAR:
    case UNIQUE:
    default:
      throw new UnsupportedExpressionOperator("oper, " + oper);
    }
    return result;
  }

  public static PredExp[] createPredExp(String binName, RelationalOperatorName oper,
      DataType literalDatType, String literal) {
    PredExp[] result = null;
    switch (oper) {
    case EQUALS:
      result = createEqualsPredicate(binName, oper, literalDatType, literal);
      break;
    case GREATER_THAN:
      result = createGreaterThanPredicate(binName, oper, literalDatType, literal);
      break;
    case GREATER_THAN_EQUALS:
      result = createGreaterThanEqualsPredicate(binName, oper, literalDatType, literal);
      break;
    case LESS_THAN:
      result = createLessThanPredicate(binName, oper, literalDatType, literal);
      break;
    case LESS_THAN_EQUALS:
      result = createLessThanEqualsPredicate(binName, oper, literalDatType, literal);
      break;
    case NOT_EQUALS:
      result = createNotEqualsPredicate(binName, oper, literalDatType, literal);
      break;
    default:
      throw new UnsupportedExpressionOperator("oper, " + oper);
    }
    return result;
  }

  public static PredExp[] createEqualsPredicate(String binName, RelationalOperatorName oper,
      DataType literalDatType, String literal) {
    DataFlavor flavor = DataFlavor.fromDataType(literalDatType);
    switch (flavor) {
    case integral:
      switch (literalDatType) {
      case Boolean:
      case Int:
      case Integer:
      case Short:
        PredExp[] integExprs = { PredExp.integerBin(binName),
            PredExp.integerValue(Integer.valueOf(literal)), PredExp.integerEqual() };
        return integExprs;
      case UnsignedInt:
      case Long:
      case UnsignedLong:
      default:
        throw new UnsupportedExpressionDataType("datatype, " + literalDatType);
      }
    case string:
      PredExp[] strExprs = { PredExp.stringBin(binName), PredExp.stringValue(literal),
          PredExp.stringEqual() };
      return strExprs;
    case temporal:
      switch (literalDatType) {
      case DateTime:
        PredExp[] strExprs2 = { PredExp.stringBin(binName), PredExp.stringValue(literal),
            PredExp.stringEqual() };
        return strExprs2;
      default:
        throw new UnsupportedExpressionDataType("datatype, " + literalDatType);
      }
    case real:
    case other:
    default:
      throw new UnsupportedExpressionDataFlavor("flavor, " + flavor);
    }
  }

  public static PredExp[] createNotEqualsPredicate(String binName, RelationalOperatorName oper,
      DataType literalDatType, String literal) {
    DataFlavor flavor = DataFlavor.fromDataType(literalDatType);
    switch (flavor) {
    case integral:
      switch (literalDatType) {
      case Boolean:
      case Int:
      case Integer:
      case Short:
        PredExp[] integExprs = { PredExp.integerBin(binName),
            PredExp.integerValue(Integer.valueOf(literal)), PredExp.integerUnequal() };
        return integExprs;
      case UnsignedInt:
      case Long:
      case UnsignedLong:
      default:
        throw new UnsupportedExpressionDataType("datatype, " + literalDatType);
      }
    case string:
      PredExp[] strExprs = { PredExp.stringBin(binName), PredExp.stringValue(literal),
          PredExp.stringUnequal() };
      return strExprs;
    case temporal:
    case real:
    case other:
    default:
      throw new UnsupportedExpressionDataFlavor("flavor, " + flavor);
    }
  }

  public static PredExp[] createGreaterThanPredicate(String binName, RelationalOperatorName oper,
      DataType literalDatType, String literal) {
    DataFlavor flavor = DataFlavor.fromDataType(literalDatType);
    switch (flavor) {
    case integral:
      PredExp[] integExprs = { PredExp.integerBin(binName),
          PredExp.integerValue(Integer.valueOf(literal)), PredExp.integerGreater() };
      return integExprs;
    case string:
    case temporal:
    case real:
    case other:
    default:
      throw new UnsupportedExpressionDataFlavor("flavor, " + flavor);
    }
  }

  public static PredExp[] createGreaterThanEqualsPredicate(String binName,
      RelationalOperatorName oper, DataType literalDatType, String literal) {
    DataFlavor flavor = DataFlavor.fromDataType(literalDatType);
    switch (flavor) {
    case integral:
      PredExp[] integExprs = { PredExp.integerBin(binName),
          PredExp.integerValue(Integer.valueOf(literal)), PredExp.integerGreaterEq() };
      return integExprs;
    case string:
    case temporal:
    case real:
    case other:
    default:
      throw new UnsupportedExpressionDataFlavor("flavor, " + flavor);
    }
  }

  public static PredExp[] createLessThanPredicate(String binName, RelationalOperatorName oper,
      DataType literalDatType, String literal) {
    DataFlavor flavor = DataFlavor.fromDataType(literalDatType);
    switch (flavor) {
    case integral:
      PredExp[] integExprs = { PredExp.integerBin(binName),
          PredExp.integerValue(Integer.valueOf(literal)), PredExp.integerLess() };
      return integExprs;
    case string:
    case temporal:
    case real:
    case other:
    default:
      throw new UnsupportedExpressionDataFlavor("flavor, " + flavor);
    }
  }

  public static PredExp[] createLessThanEqualsPredicate(String binName,
      RelationalOperatorName oper, DataType literalDatType, String literal) {
    DataFlavor flavor = DataFlavor.fromDataType(literalDatType);
    switch (flavor) {
    case integral:
      PredExp[] integExprs = { PredExp.integerBin(binName),
          PredExp.integerValue(Integer.valueOf(literal)), PredExp.integerLessEq() };
      return integExprs;
    case string:
    case temporal:
    case real:
    case other:
    default:
      throw new UnsupportedExpressionDataFlavor("flavor, " + flavor);
    }
  }

  public static PredExp[] createRegExpPredicate(String binName, PredicateOperatorName oper,
      DataType literalDatType, String literal) {
    DataFlavor flavor = DataFlavor.fromDataType(literalDatType);
    switch (flavor) {
    case string:
      String regexp = literal.replace("*", ".*");
      PredExp[] strExprs = { PredExp.stringBin(binName), PredExp.stringValue(regexp),
          PredExp.stringRegex(RegexFlag.ICASE | RegexFlag.NEWLINE) };
      return strExprs;
    case integral:
    case temporal:
    case real:
    case other:
    default:
      throw new UnsupportedExpressionDataFlavor("flavor, " + flavor);
    }
  }

}
