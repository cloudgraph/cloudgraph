package org.cloudgraph.bigtable.client;

import org.cloudgraph.core.client.TableName;
import org.cloudgraph.hbase.client.HBaseClientFactory;
import org.cloudgraph.hbase.client.HBaseTableName;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

public class BigTableClientFactory extends HBaseClientFactory {
  @Override
  public TableName createTableName(String tableNamespace, String tableName) {
    return BigTableTableName.valueOf(tableNamespace, tableName);
  }

  @Override
  public TableName createTableName(TableMapping table, StoreMappingContext context) {
    String namespace = this.createPhysicalNamespace(HBaseTableName.PHYSICAL_NAME_DELIMITER, table,
        context);
    return BigTableTableName.valueOf(namespace, table.getTable().getName());
  }

}
