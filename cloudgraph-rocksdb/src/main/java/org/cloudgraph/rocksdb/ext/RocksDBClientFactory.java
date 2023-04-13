package org.cloudgraph.rocksdb.ext;

import java.io.IOException;

import org.cloudgraph.core.client.CellUtil;
import org.cloudgraph.core.client.CellValues;
import org.cloudgraph.core.client.ClientFactory;
import org.cloudgraph.core.client.Delete;
import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.client.FilterList;
import org.cloudgraph.core.client.FilterList.Operator;
import org.cloudgraph.core.client.DefaultClientFactory;
import org.cloudgraph.core.client.Get;
import org.cloudgraph.core.client.Increment;
import org.cloudgraph.core.client.Put;
import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.client.RowMutations;
import org.cloudgraph.core.client.Scan;
import org.cloudgraph.core.client.TableName;
import org.cloudgraph.core.scan.CompleteRowKey;
import org.cloudgraph.core.scan.FuzzyRowKey;
import org.cloudgraph.core.scan.PartialRowKey;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

public class RocksDBClientFactory extends DefaultClientFactory implements ClientFactory {

  @Override
  public Put createPut(byte[] rowKey) {
    return new RocksDBPut(rowKey);
  }

  @Override
  public Delete createDelete(byte[] rowKey) {
    return new RocksDBDel(rowKey);
  }

  @Override
  public Increment createIncrement(byte[] rowKey) {
    return new RocksDBIncrement(rowKey);
  }

  @Override
  public Get createGet(byte[] rowKey) {
    return new RocksDBGet(rowKey);
  }

  @Override
  public CellValues createCellValues(byte[] rowKey) {
    return new RocksDBCellValues(rowKey);
  }

  @Override
  public CellUtil getCellUtil() {
    return null;
  }

  @Override
  public RowMutations createRowMutations(byte[] row) {
    return new RocksDBRowMutations(row);
  }

  @Override
  public CellValues createCellValues(Result result) {
    return new RocksDBCellValues(result);
  }

  @Override
  public Scan createPartialRowKeyScan(PartialRowKey partialRowKey, Filter columnFilter) {

    RocksDBScan scan = new RocksDBScan(partialRowKey.getStartKey(), partialRowKey.getStopKey(),
        columnFilter);
    return scan;
  }

  @Override
  public Get createGet(CompleteRowKey rowKey, Filter columnFilter) {
    return new RocksDBGet(rowKey.getKey(), columnFilter);
  }

  @Override
  public Scan createScan(Filter fuzzyRowFilter, Filter columnFilter) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public Scan createScan(Float sample, Filter columnFilter) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public Scan createScan(Filter columnFilter) {
    RocksDBScan scan = new RocksDBScan(columnFilter);
    return scan;
  }

  @Override
  public Scan createScan(FuzzyRowKey fuzzyScan, Filter columnFilter) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public Scan createScan(Scan scan) throws IOException {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public TableName createTableName(String tableNamespace, String tableName) {
    return RocksDBTableName.valueOf(tableNamespace, tableName);
  }

  @Override
  public TableName createTableName(TableMapping table, StoreMappingContext context) {
    throw new IllegalStateException("not implemented");
  }

  @Override
  public String getNamespaceQualifiedPhysicalName(TableMapping tableConfig,
      StoreMappingContext storeMapping) {
    String name = this.createPhysicalNamespaceQualifiedPhysicalName(
        RocksDBTableName.PHYSICAL_NAME_DELIMITER, tableConfig, storeMapping);
    return name;
  }

  @Override
  public String getQualifiedPhysicalTableNamespace(TableMapping tableConfig,
      StoreMappingContext storeMapping) {
    String namespace = this.createPhysicalNamespace(RocksDBTableName.PHYSICAL_NAME_DELIMITER,
        tableConfig, storeMapping);
    return namespace;
  }

}
