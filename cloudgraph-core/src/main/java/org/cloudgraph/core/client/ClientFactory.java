package org.cloudgraph.core.client;

import java.io.IOException;

import org.cloudgraph.core.client.FilterList.Operator;
import org.cloudgraph.core.scan.CompleteRowKey;
import org.cloudgraph.core.scan.FuzzyRowKey;
import org.cloudgraph.core.scan.PartialRowKey;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

public interface ClientFactory {

  Put createPut(byte[] rowKey);

  Delete createDelete(byte[] rowKey);

  Increment createIncrement(byte[] rowKey);

  Get createGet(byte[] rowKey);

  CellValues createCellValues(byte[] rowKey);

  CellUtil getCellUtil();

  RowMutations createRowMutations(byte[] row);

  CellValues createCellValues(Result result);

  Scan createPartialRowKeyScan(PartialRowKey partialRowKey, Filter columnFilter);

  Get createGet(CompleteRowKey rowKey, Filter columnFilter);

  Scan createScan(Filter fuzzyRowFilter, Filter columnFilter);

  Scan createScan(Float sample, Filter columnFilter);

  Scan createScan(Filter columnFilter);

  Scan createScan(FuzzyRowKey fuzzyRowKey, Filter columnFilter);

  Scan createScan(Scan scan) throws IOException;

  TableName createTableName(TableMapping table, StoreMappingContext context);

  TableName createTableName(String tableNamespace, String tableName);

  String getNamespaceQualifiedPhysicalName(TableMapping tableConfig,
      StoreMappingContext storeMapping);

  String getQualifiedPhysicalTableNamespace(TableMapping tableConfig,
      StoreMappingContext storeMapping);
}
