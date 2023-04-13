package org.cloudgraph.maprdb.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.cloudgraph.core.client.CellUtil;
import org.cloudgraph.core.client.CellValues;
import org.cloudgraph.core.client.ClientFactory;
import org.cloudgraph.core.client.Delete;
import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.client.FilterList;
import org.cloudgraph.core.client.FilterList.Operator;
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
import org.cloudgraph.hbase.client.HBaseClientFactory;
import org.cloudgraph.hbase.client.HBaseDelete;
import org.cloudgraph.hbase.client.HBaseFilter;
import org.cloudgraph.hbase.client.HBaseGet;
import org.cloudgraph.hbase.client.HBaseIncrement;
import org.cloudgraph.hbase.client.HBasePut;
import org.cloudgraph.hbase.client.HBaseRowMutations;
import org.cloudgraph.hbase.client.HBaseTableName;
import org.cloudgraph.hbase.io.HBaseCellValues;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

public class MaprDBClientFactory extends HBaseClientFactory implements ClientFactory {

  @Override
  public TableName createTableName(String tableNamespace, String tableName) {
    return MaprDBTableName.valueOf(tableNamespace, tableName);
  }

  @Override
  public TableName createTableName(TableMapping table, StoreMappingContext context) {
    String namespace = this.createPhysicalNamespace(MaprDBTableName.PHYSICAL_NAME_DELIMITER, table,
        context);
    return MaprDBTableName.valueOf(namespace, table.getTable().getName());
  }

  @Override
  public String getNamespaceQualifiedPhysicalName(TableMapping tableConfig,
      StoreMappingContext storeMapping) {
    String name = this.createPhysicalNamespaceQualifiedPhysicalName(
        MaprDBTableName.PHYSICAL_NAME_DELIMITER, tableConfig, storeMapping);
    return name;
  }

  @Override
  public String getQualifiedPhysicalTableNamespace(TableMapping tableConfig,
      StoreMappingContext storeMapping) {
    String namespace = this.createPhysicalNamespace(MaprDBTableName.PHYSICAL_NAME_DELIMITER,
        tableConfig, storeMapping);
    return namespace;
  }

}
