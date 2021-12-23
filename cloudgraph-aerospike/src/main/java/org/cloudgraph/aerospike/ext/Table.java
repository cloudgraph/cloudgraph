package org.cloudgraph.aerospike.ext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.aerospike.filter.ColumnInfo;
import org.cloudgraph.aerospike.key.CompositeColumnKeyFactory;
import org.cloudgraph.aerospike.scan.ScanLiteral;
import org.cloudgraph.aerospike.scan.WildcardStringLiteral;
import org.cloudgraph.common.Bytes;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.plasma.query.model.PredicateOperatorName;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.RegexFlag;
import com.aerospike.client.query.Statement;

public class Table implements TableName {
  private static Log log = LogFactory.getLog(Table.class);
  private String namespace;
  private String qualifiedName;
  private String setName;
  private AerospikeClient client;
  private TableMapping tableMapping;
  private StoreMappingContext mappingContext;

  @SuppressWarnings("unused")
  private Table() {
  }

  public Table(TableName tableName, AerospikeClient client, TableMapping tableMapping,
      StoreMappingContext mappingContext) {
    this.namespace = tableName.getNamespace();
    this.qualifiedName = tableName.getTableName();
    this.setName = this.qualifiedName;
    int idx = tableName.getTableName().lastIndexOf("/"); // FIXME: configured??
                                                         // why??
    if (idx != -1)
      this.setName = this.qualifiedName.substring(idx + 1);
    this.client = client;
    this.tableMapping = tableMapping;
    this.mappingContext = mappingContext;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getSetName() {
    return setName;
  }

  public void batch(List<Row> rows, Object[] results) throws AerospikeException {
    WritePolicy writePolicy = new WritePolicy();
    // writePolicy.expiration = 20;
    // no put() for multiple keys like there is for get()
    for (Row row : rows) {
      // if (log.isDebugEnabled())
      Key key = row.getKey(this);
      if (log.isDebugEnabled())
        log.debug("writing row: '" + key + "' columns: " + Arrays.toString(row.getColumnNames()));
      this.client.put(writePolicy, key, row.getBins());
    }
  }

  @Override
  public String getTableName() {
    return getSetName();
  }

  public Result get(Get get) {
    Policy policy = new Policy();
    String[] names = get.getColumnNames();
    Key key = get.getKey(this);
    if (log.isDebugEnabled())
      log.debug("reading row: '" + key + "'");
    Record rec = this.client.get(policy, key, names);
    KeyInfo ki = new KeyInfo(key, this.tableMapping.getDataColumnFamilyName());
    return new Result(ki, rec, get.getColumnFilter());
  }

  public Result[] get(List<Get> gets, Table table) {
    BatchPolicy policy = new BatchPolicy();
    Key[] keys = new Key[gets.size()];
    for (int i = 0; i < gets.size(); i++) {
      keys[i] = gets.get(i).getKey(this);
      if (log.isDebugEnabled())
        log.debug("reading row: '" + keys[i] + "'");
    }
    org.cloudgraph.aerospike.filter.Filter columnFilter = gets.get(0).getColumnFilter();
    Record[] recs = this.client.get(policy, keys);
    Result[] results = new Result[recs.length];
    int i = 0;
    for (Record rec : recs) {
      KeyInfo ki = new KeyInfo(keys[i], this.tableMapping.getDataColumnFamilyName());
      results[i] = new Result(ki, rec, columnFilter);
      i++;
    }
    return results;
  }

  public RecordSet scan(Scan scan) {
    QueryPolicy queryPolicy = new QueryPolicy();
    Statement stmt = new Statement();
    stmt.setSetName(this.getSetName());
    stmt.setNamespace(this.getNamespace());

    Map<String, DataType> typeMap = new HashMap<>();
    List<String> names = new ArrayList<>();
    for (String binName : scan.getColumnFilter().getColumnKeys()) {
      names.add(binName);
      ColumnInfo col = scan.getColumnFilter().getColumn(binName);
      DataType binDataType = DataType.String;
      if (col.hasProperty()) {
        if (col.getProperty().getType().isDataType()) {
          DataType colDataType = DataType.valueOf(col.getProperty().getType().getName());
          binDataType = colDataType;
        } else {
          binDataType = DataType.String;
        }
      } else {
        binDataType = DataType.String;
      }
      if (log.isDebugEnabled())
        log.debug("scan added bin: " + binName + " (" + binDataType + ")");
      typeMap.put(binName, binDataType);
    }
    String[] binNames = new String[names.size()];
    names.toArray(binNames);
    stmt.setBinNames(binNames);

    List<PredExp> predicatesExprs = new ArrayList<>();
    int predicateCount = 0;
    if (scan.hasScanLiterals()) {
      for (ScanLiteral scanLit : scan.getScanLiterals().getLiterals()) {
        CompositeColumnKeyFactory ccf = new CompositeColumnKeyFactory(scanLit.getRootType(),
            this.mappingContext);
        byte[] binBytes = ccf.createColumnKey(scanLit.getRootType(), scanLit.getProperty());
        String binName = Bytes.toString(binBytes);
        DataType dataType = DataType.valueOf(scanLit.getProperty().getType().getName());
        if (scanLit.getRelationalOperator() != null) {
          PredExp[] exprs = this.createPredExp(binName, scanLit.getRelationalOperator(), dataType,
              scanLit.getLiteral());
          for (PredExp ex : exprs)
            predicatesExprs.add(ex);

          predicateCount++;
        } else {
          if (WildcardStringLiteral.class.isInstance(scanLit)) {
            WildcardStringLiteral wildcardLit = WildcardStringLiteral.class.cast(scanLit);
            PredExp[] exprs = this.createPredExp(binName, wildcardLit.getWildcardOperator()
                .getValue(), dataType, scanLit.getLiteral());
            for (PredExp ex : exprs)
              predicatesExprs.add(ex);

            predicateCount++;
          } else
            log.warn("skipped scan literal: " + scanLit);
        }

        // stmt.setFilter(Filter.equal(Bytes.toString(binBytes),
      }
    }

    if (predicateCount > 0) {
      if (predicateCount > 1) {
        predicatesExprs.add(PredExp.and(predicateCount));
      }
      PredExp[] exprs = new PredExp[predicatesExprs.size()];
      predicatesExprs.toArray(exprs);
      stmt.setPredExp(exprs);
    }

    RecordSet recordSet = this.client.query(queryPolicy, stmt);
    return recordSet;
  }

  private PredExp[] createPredExp(String binName, PredicateOperatorName oper,
      DataType literalDatType, String literal) {
    PredExp[] result = null;
    switch (oper) {
    case LIKE:
      result = this.createRegExpPredicate(binName, oper, literalDatType, literal);
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
      throw new IllegalArgumentException("unknown oper, " + oper);
    }
    return result;
  }

  private PredExp[] createPredExp(String binName, RelationalOperatorName oper,
      DataType literalDatType, String literal) {
    PredExp[] result = null;
    switch (oper) {
    case EQUALS:
      result = this.createEqualsPredicate(binName, oper, literalDatType, literal);
      break;
    case GREATER_THAN:
      result = this.createGreaterThanPredicate(binName, oper, literalDatType, literal);
      break;
    case GREATER_THAN_EQUALS:
      result = this.createGreaterThanEqualsPredicate(binName, oper, literalDatType, literal);
      break;
    case LESS_THAN:
      result = this.createLessThanPredicate(binName, oper, literalDatType, literal);
      break;
    case LESS_THAN_EQUALS:
      result = this.createLessThanEqualsPredicate(binName, oper, literalDatType, literal);
      break;
    case NOT_EQUALS:
      result = this.createNotEqualsPredicate(binName, oper, literalDatType, literal);
      break;
    default:
      throw new IllegalArgumentException("unknown oper, " + oper);
    }
    return result;
  }

  private PredExp[] createEqualsPredicate(String binName, RelationalOperatorName oper,
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
        throw new IllegalArgumentException("unknown datatype, " + literalDatType);
      }
    case string:
      PredExp[] strExprs = { PredExp.stringBin(binName), PredExp.stringValue(literal),
          PredExp.stringEqual() };
      return strExprs;
    case temporal:
    case real:
    case other:
    default:
      throw new IllegalArgumentException("unknown flavor, " + flavor);
    }
  }

  private PredExp[] createNotEqualsPredicate(String binName, RelationalOperatorName oper,
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
        throw new IllegalArgumentException("unknown datatype, " + literalDatType);
      }
    case string:
      PredExp[] strExprs = { PredExp.stringBin(binName), PredExp.stringValue(literal),
          PredExp.stringUnequal() };
      return strExprs;
    case temporal:
    case real:
    case other:
    default:
      throw new IllegalArgumentException("unknown flavor, " + flavor);
    }
  }

  private PredExp[] createGreaterThanPredicate(String binName, RelationalOperatorName oper,
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
      throw new IllegalArgumentException("unknown flavor, " + flavor);
    }
  }

  private PredExp[] createGreaterThanEqualsPredicate(String binName, RelationalOperatorName oper,
      DataType literalDatType, String literal) {
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
      throw new IllegalArgumentException("unknown flavor, " + flavor);
    }
  }

  private PredExp[] createLessThanPredicate(String binName, RelationalOperatorName oper,
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
      throw new IllegalArgumentException("unknown flavor, " + flavor);
    }
  }

  private PredExp[] createLessThanEqualsPredicate(String binName, RelationalOperatorName oper,
      DataType literalDatType, String literal) {
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
      throw new IllegalArgumentException("unknown flavor, " + flavor);
    }
  }

  private PredExp[] createRegExpPredicate(String binName, PredicateOperatorName oper,
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
      throw new IllegalArgumentException("unknown flavor, " + flavor);
    }
  }

  @Override
  public String toString() {
    return "Table [namespace=" + namespace + ", setName=" + setName + "]";
  }

}
