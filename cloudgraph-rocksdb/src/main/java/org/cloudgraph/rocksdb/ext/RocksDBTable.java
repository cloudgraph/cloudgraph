package org.cloudgraph.rocksdb.ext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.Bytes;
import org.cloudgraph.core.client.CompareOper;
import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.client.Get;
import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.client.ResultScanner;
import org.cloudgraph.core.client.Row;
import org.cloudgraph.core.client.RowMutations;
import org.cloudgraph.core.client.Scan;
import org.cloudgraph.core.client.Table;
import org.cloudgraph.core.client.TableName;
import org.cloudgraph.rocksdb.filter.FilterList;
import org.cloudgraph.rocksdb.filter.MultiColumnCompareFilter;
import org.cloudgraph.rocksdb.filter.MultiColumnPrefixFilter;
//import org.cloudgraph.rocksdb.filter.Filter;
//import org.cloudgraph.rocksdb.key.CompositeColumnKeyFactory;
import org.cloudgraph.rocksdb.proto.TupleCodec;
//import org.cloudgraph.rocksdb.scan.ScanLiteral;
//import org.cloudgraph.rocksdb.scan.WildcardStringLiteral;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.service.GraphServiceException;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksDBTable implements Table, TableName {
  private static Log log = LogFactory.getLog(RocksDBTable.class);
  private String namespace;
  private String qualifiedName;
  private String setName;
  private RocksDB client;
  private TableMapping tableMapping;
  private StoreMappingContext mappingContext;

  @SuppressWarnings("unused")
  private RocksDBTable() {
  }

  public RocksDBTable(TableName tableName, RocksDB client, TableMapping tableMapping,
      StoreMappingContext mappingContext) {
    this.namespace = tableName.getNamespace();
    this.qualifiedName = tableName.getTableName();
    this.setName = this.qualifiedName;
    int idx = tableName.getTableName().lastIndexOf("/"); // FIXME:
    // configured??
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

  public void batch(List<Row> rows, Object[] results) {
    // FIXME:
    // because of the interim column serialization impl
    // must correlate columns for puts in the batch
    // with columns used to replace a row for a partial
    // delete. Until columns are
    // pulled out into separate storage rows, just write
    // all puts before any deletes and use 1 writes.
    this.writeIncrements(rows, results);
    this.writePuts(rows, results);
    this.writeDeletes(rows, results);
  }

  public void writeIncrements(List<Row> rows, Object[] results) {
    WriteOptions writeOpt = new WriteOptions();
    writeOpt.setSync(true);
    writeOpt.setDisableWAL(false);
    try {
      for (Row row : rows) {
        RocksDBRowMutation mutation = RocksDBRowMutation.class.cast(row); // FIXME:
        // cast
        switch (mutation.getMutationType()) {
        case INCREMENT:
          Column[] columns = mutation.getColumns();
          Key key = mutation.getKey(this);
          int col = 0;
          for (Column column : columns) {
            this.client.merge(writeOpt, key.getData(), column.getData());
            byte[] merged = this.client.get(key.getData()); // FIXME which
                                                            // column
            results[col] = merged; // FIXME: or convert to long?
            col++;
          }
          break;
        case PUT:
          break;
        case DEL:
          break;
        default:
          throw new IllegalArgumentException("unknown mutation tape, " + mutation.getMutationType());
        }
      }
    } catch (RocksDBException e) {
      throw new GraphServiceException(e);
    }
  }

  public void writePuts(List<Row> rows, Object[] results) {
    WriteOptions writeOpt = new WriteOptions();
    writeOpt.setSync(true);
    writeOpt.setDisableWAL(false);
    WriteBatch batch = new WriteBatch();
    try {
      for (Row row : rows) {
        RocksDBRowMutation mutation = RocksDBRowMutation.class.cast(row); // FIXME:
        // cast
        switch (mutation.getMutationType()) {
        case PUT:
          Column[] columns = mutation.getColumns();
          Key key = mutation.getKey(this);
          byte[] existingValue;
          try {
            existingValue = this.client.get(key.getData());
          } catch (RocksDBException e) {
            throw new GraphServiceException(e);
          }
          // merge
          if (existingValue != null) {
            Column[] existingColumns = new Column[0];
            existingColumns = TupleCodec.decodeRow(existingValue);
            Map<String, Column> commitMap = new HashMap<>();
            for (Column column : existingColumns)
              commitMap.put(column.getName(), column);
            for (Column column : columns)
              commitMap.put(column.getName(), column);

            columns = new Column[commitMap.size()];
            commitMap.values().toArray(columns);
          }

          if (log.isDebugEnabled())
            log.debug("batch/put: " + this.toDebugString(key, columns, true));
          byte[] rowBytes = TupleCodec.encodeRow(columns);
          batch.put(mutation.getRow(), rowBytes);
          break;
        case DEL:
          break;
        default:
          throw new IllegalArgumentException("unknown mutation tape, " + mutation.getMutationType());
        }
      }

      // write puts
      this.client.write(writeOpt, batch);

    } catch (RocksDBException e) {
      throw new GraphServiceException(e);
    }
  }

  public void writeDeletes(List<Row> rows, Object[] results) {
    WriteOptions writeOpt = new WriteOptions();
    writeOpt.setSync(true);
    writeOpt.setDisableWAL(false);
    WriteBatch batch = new WriteBatch();
    try {
      for (Row row : rows) {
        RocksDBRowMutation mutation = RocksDBRowMutation.class.cast(row); // FIXME:
        Key key = mutation.getKey(this);
        switch (mutation.getMutationType()) {
        case DEL:
          Column[] deleteCols = mutation.getColumns();
          key = RocksDBRow.class.cast(row).getKey(this);
          if (deleteCols != null && deleteCols.length > 0) {
            byte[] existingValue;
            try {
              existingValue = this.client.get(key.getData());
            } catch (RocksDBException e) {
              throw new GraphServiceException(e);
            }
            if (existingValue != null) {
              Column[] existingColumns = new Column[0];
              existingColumns = TupleCodec.decodeRow(existingValue);
              Map<String, Column> commitMap = new HashMap<>();
              for (Column column : existingColumns)
                commitMap.put(column.getName(), column);
              List<Column> removed = new ArrayList<>();
              ;
              for (Column deleteCol : deleteCols) {
                Column col = commitMap.remove(deleteCol.getName());
                if (col != null)
                  removed.add(col);
              }
              if (log.isDebugEnabled())
                log.debug("batch/delete remove: " + this.toDebugString(key, removed));
              Column[] columns = new Column[commitMap.size()];
              commitMap.values().toArray(columns);
              if (log.isDebugEnabled())
                log.debug("batch/delete write: " + this.toDebugString(key, columns, true));
              byte[] rowBytes = TupleCodec.encodeRow(columns);
              batch.put(mutation.getRow(), rowBytes);
            } else {
              throw new IllegalArgumentException("expected existing delete row: " + key);
            }
          } else {
            // no columns specified so wipe the row
            if (log.isDebugEnabled())
              log.debug("batch/delete row: " + key);
            batch.delete(mutation.getRow());
          }

          break;
        case PUT:
          break; // see above
        default:
          throw new IllegalArgumentException("unknown mutation tape, " + mutation.getMutationType());
        }
      }

      // write deletes
      this.client.write(writeOpt, batch);

    } catch (RocksDBException e) {
      throw new GraphServiceException(e);
    }
  }

  @Override
  public String getTableName() {
    return getSetName();
  }

  public Result get(Get get) {
    if (get.getFilter() == null)
      log.warn("expected column filter");
    Key key = RocksDBGet.class.cast(get).getKey(this);
    byte[] value;
    try {
      value = this.client.get(key.getData());
    } catch (RocksDBException e) {
      throw new GraphServiceException(e);
    }
    Column[] columns = new Column[0];
    if (value != null) {
      columns = TupleCodec.decodeRow(value);
      if (log.isDebugEnabled())
        log.debug("get: " + this.toDebugString(key, columns, true));
      if (get.getFilter() != null)
        columns = this.filter(columns, get.getFilter());
      if (log.isDebugEnabled())
        log.debug("filtered get: " + this.toDebugString(key, columns, true));
    }

    KeyInfo ki = new KeyInfo(key, this.tableMapping.getDataColumnFamilyName());
    return new RocksDBResult(ki, columns);
  }

  public Result[] get(List<Get> gets) {
    List<byte[]> keys = new ArrayList<>(gets.size());
    for (int i = 0; i < gets.size(); i++) {
      keys.add(gets.get(i).getRow());
    }
    Filter columnFilter = gets.get(0).getFilter();
    List<byte[]> values;
    try {
      values = this.client.multiGetAsList(keys);
    } catch (RocksDBException e) {
      throw new GraphServiceException(e);
    }
    Result[] results = new Result[values.size()];
    int i = 0;
    for (byte[] value : values) {
      Key key = new Key(keys.get(i));
      Column[] columns = TupleCodec.decodeRow(value);
      columns = this.filter(columns, columnFilter);
      if (log.isDebugEnabled())
        log.debug("mget: " + this.toDebugString(key, columns, true));
      KeyInfo ki = new KeyInfo(key, this.tableMapping.getDataColumnFamilyName());
      results[i] = new RocksDBResult(ki, columns);
      i++;
    }
    return results;
  }

  public Result[] scan(Scan scan) {

    List<Result> list = new ArrayList<>();
    try {
      ReadOptions readOpts = new ReadOptions();
      Slice lower = new Slice(scan.getStartRow());
      readOpts.setIterateLowerBound(lower);
      Slice upper = new Slice(scan.getStopRow());
      readOpts.setIterateUpperBound(upper);
      if (log.isDebugEnabled())
        log.debug("scan lower/upper: '" + Bytes.toString(scan.getStartRow()) + "'/'"
            + Bytes.toString(scan.getStopRow()) + "'");
      RocksIterator iterator = this.client.newIterator(readOpts);
      byte[] prevIterKey = null;
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        iterator.status();
        byte[] iterKey = iterator.key();
        if (log.isDebugEnabled())
          log.debug("iter key: " + Bytes.toString(iterKey));
        if (prevIterKey != null) {
          org.cloudgraph.rocksdb.ext.Bytes prevBytes = new org.cloudgraph.rocksdb.ext.Bytes(
              prevIterKey);
          org.cloudgraph.rocksdb.ext.Bytes currBytes = new org.cloudgraph.rocksdb.ext.Bytes(iterKey);
          int compare = currBytes.compareTo(prevBytes);
          if (compare != 1)
            log.warn("expected ordered key");
        }
        Key key = new Key(iterKey);
        KeyInfo ki = new KeyInfo(key, this.tableMapping.getDataColumnFamilyName());
        byte[] value = iterator.value();
        Column[] columns = TupleCodec.decodeRow(value);
        columns = this.filter(columns, scan.getFilter());
        if (log.isDebugEnabled())
          log.debug("scan: " + this.toDebugString(key, columns, true));
        Result result = new RocksDBResult(ki, columns);
        list.add(result);
        prevIterKey = iterKey;
      }
    } catch (RocksDBException e) {
      throw new GraphServiceException(e);
    }
    Result[] results = new Result[list.size()];
    list.toArray(results);
    return results;
  }

  // FIXME: quick and dirty column filtering
  private Column[] filter(Column[] columns, Filter filter) {
    org.cloudgraph.rocksdb.filter.Filter rocksColumnFilter = RocksDBFilter.class.cast(filter).get();

    List<Column> filtered = new ArrayList<>();
    if (!FilterList.class.isInstance(rocksColumnFilter)) {
      if (MultiColumnPrefixFilter.class.isInstance(rocksColumnFilter)) {
        MultiColumnPrefixFilter prefixFilter = MultiColumnPrefixFilter.class
            .cast(rocksColumnFilter);
        for (String filterName : prefixFilter.getColumnNames()) {
          for (Column col : columns) {
            if (col.getName().startsWith(filterName)) {
              filtered.add(col);
              // if (log.isTraceEnabled())
              // log.trace("prefix filter/added '" + col.getName()
              // + "' (" +
              // filterName + ")");
            }
          }
        }
      } else if (MultiColumnCompareFilter.class.isInstance(rocksColumnFilter)) {
        MultiColumnCompareFilter perfixFilter = MultiColumnCompareFilter.class
            .cast(rocksColumnFilter);
        for (String filterName : perfixFilter.getColumnNames()) {
          for (Column col : columns) {
            if (col.getName().equals(filterName)) { // FIXME:
              // compare oper?
              filtered.add(col);
              // if (log.isTraceEnabled())
              // log.trace("prefix filter/added '" + col.getName()
              // + "' (" +
              // filterName + ")");
            }
          }
        }
      } else {
        throw new IllegalStateException("unknown filter class, " + rocksColumnFilter.getClass());
      }
    } else {
      throw new IllegalStateException("unknown filter class, " + rocksColumnFilter.getClass());
    }
    columns = new Column[filtered.size()];
    filtered.toArray(columns);
    // if (log.isTraceEnabled())
    // log.trace("filtered " + filtered.size() + " columns");
    return columns;
  }

  private String toDebugString(Key key, List<Column> columns) {
    return this.toDebugString(key, columns, false);
  }

  private String toDebugString(Key key, List<Column> columns, boolean values) {
    StringBuilder buf = new StringBuilder();
    buf.append(key);
    buf.append(" [");
    int i = 0;
    for (Column col : columns) {
      if (i > 0)
        buf.append(", ");
      buf.append(toDebugString(col, values));
      i++;
    }
    buf.append("]");
    return buf.toString();
  }

  private String toDebugString(Key key, Column[] columns) {
    return this.toDebugString(key, columns, true);
  }

  private String toDebugString(Key key, Column[] columns, boolean values) {
    StringBuilder buf = new StringBuilder();
    buf.append(key);
    buf.append(" [");
    int i = 0;
    for (Column col : columns) {
      if (i > 0)
        buf.append(", ");
      buf.append(toDebugString(col, values));
      i++;
    }
    buf.append("]");
    return buf.toString();
  }

  private String toDebugString(Column column, boolean values) {
    StringBuilder buf = new StringBuilder();
    buf.append(column.getName());
    if (values) {
      buf.append("='");
      String value = column.getDataDebugString(); // Bytes.toString(column.getData());
      value = value.replace("  ", "");
      value = value.replace('\n', ' ');
      if (value.length() > 60) {
        value = value.substring(0, 60);
        value = value + "...";
      }
      buf.append(value);
      buf.append("'");
    }
    return buf.toString();
  }

  @Override
  public String toString() {
    return "Table [namespace=" + namespace + ", setName=" + setName + "]";
  }

  @Override
  public String getName() {
    return this.getTableName();
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] checkFamBytes, byte[] checkQualBytes,
      CompareOper oper, byte[] checkValueBytes, RowMutations checkedMutations) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return new RocksDBResultScanner();
  }

  @Override
  public Object getConfiguration() {
    return new Properties();
  }

  @Override
  public String getQualifiedLogicalName(StoreMappingContext mappingContext) {
    // TODO Auto-generated method stub
    return null;
  }

}
