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

public class MaprDBClientFactory extends HBaseClientFactory implements ClientFactory {

  @Override
  public TableName createTableName(String tableNamespace, String tableName) {
    return MaprDBTableName.valueOf(tableNamespace, tableName);
  }

}
