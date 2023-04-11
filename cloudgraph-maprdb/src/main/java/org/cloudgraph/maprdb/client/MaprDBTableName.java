package org.cloudgraph.maprdb.client;

import org.cloudgraph.core.client.TableName;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

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
    return this.tablename.getQualifierAsString();
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

  @Override
  public String getQualifiedLogicalName(StoreMappingContext mappingContext) {
    StringBuilder result = new StringBuilder();
    String qualifiedNamespace = getNamespace();
    // namespace has a prefix, strip it and its delim
    // to reconstruct just the logical name
    // which will necessarily not involve the root prefix
    if (mappingContext.hasTableNamespaceRoot()) {
      if (qualifiedNamespace.startsWith(mappingContext.getTableNamespaceRoot())) {
        qualifiedNamespace = qualifiedNamespace.substring(mappingContext.getTableNamespaceRoot()
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
}
