package org.cloudgraph.maprdb.client;

import org.cloudgraph.core.client.TableName;

public class MaprDBTableName implements TableName {
  private org.apache.hadoop.hbase.TableName tablename;

  private MaprDBTableName(String qualifiedPhysicalTableNamespace, String qualifiedPhysicalTableName) {
    this.tablename = org.apache.hadoop.hbase.TableName.valueOf(qualifiedPhysicalTableNamespace,
        qualifiedPhysicalTableName);
  }

  @Override
  public String getNamespace() {
    return this.tablename.getNamespaceAsString();
  }

  @Override
  public String getTableName() {
    return this.tablename.getNameAsString();
  }

  public static MaprDBTableName valueOf(String qualifiedPhysicalTableNamespace,
      String qualifiedPhysicalTableName) {
    if (qualifiedPhysicalTableNamespace != null)
      throw new IllegalArgumentException(
          "MAPRDB tables have no namespace, but must be fully path qualified");

    if (qualifiedPhysicalTableName == null || qualifiedPhysicalTableName.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedPhysicalTableName");
    return new MaprDBTableName(qualifiedPhysicalTableNamespace, qualifiedPhysicalTableName);
  }
}
