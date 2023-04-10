package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.TableName;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

public class HBaseTableName implements TableName {
  private org.apache.hadoop.hbase.TableName tablename;

  private HBaseTableName(String qualifiedPhysicalTableNamespace, String qualifiedPhysicalTableName) {
    this.tablename = org.apache.hadoop.hbase.TableName.valueOf(qualifiedPhysicalTableNamespace,
        qualifiedPhysicalTableName);
  }

  public org.apache.hadoop.hbase.TableName get() {
    return this.tablename;
  }

  @Override
  public String getQualifiedLogicalName(StoreMappingContext mappingContext) {
    StringBuilder result = new StringBuilder();
    String qualifiedNamespace = getNamespace();
    // namespace has a prefix, strip it and its delim
    // to reconstruct just the logical name
    // which will necessarily not involve the root prefix
    if (mappingContext.hasRootTablePathPrefix()) {
      if (qualifiedNamespace.startsWith(mappingContext.getRootTablePathPrefix())) {
        qualifiedNamespace = qualifiedNamespace.substring(mappingContext.getRootTablePathPrefix()
            .length() + 1/* the delim */);
      }
    }
    // replace physical with logical delimiters
    qualifiedNamespace = qualifiedNamespace.replaceAll(TableMapping.TABLE_PHYSICAL_NAME_DELIM,
        TableMapping.TABLE_LOGICAL_NAME_DELIM);
    result.append(qualifiedNamespace);
    result.append(TableMapping.TABLE_LOGICAL_NAME_DELIM);
    result.append(getTableName());
    return result.toString();
  }

  @Override
  public String getNamespace() {
    return this.tablename.getNamespaceAsString();
  }

  @Override
  public String getTableName() {
    // Note: in HBase the getTableAsString() returns the namespace qualified
    // name as 'namespace:name'
    // and we are needing just the "qualifier"
    return this.tablename.getQualifierAsString();
  }

  public static HBaseTableName valueOf(String qualifiedPhysicalTableNamespace,
      String qualifiedPhysicalTableName) {
    if (qualifiedPhysicalTableNamespace == null
        || qualifiedPhysicalTableNamespace.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedPhysicalTableNamespace");
    if (qualifiedPhysicalTableName == null || qualifiedPhysicalTableName.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedPhysicalTableName");
    return new HBaseTableName(qualifiedPhysicalTableNamespace, qualifiedPhysicalTableName);
  }

}
