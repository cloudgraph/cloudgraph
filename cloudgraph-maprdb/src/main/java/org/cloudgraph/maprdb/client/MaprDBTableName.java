package org.cloudgraph.maprdb.client;

import org.cloudgraph.core.client.TableName;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

public class MaprDBTableName implements TableName {
  public static String PHYSICAL_NAME_DELIMITER = "/";
  private org.apache.hadoop.hbase.TableName tablename;

  private MaprDBTableName(String qualifiedPhysicalTableNamespace, String qualifiedPhysicalTableName) {
    this.tablename = org.apache.hadoop.hbase.TableName.valueOf(qualifiedPhysicalTableNamespace,
        qualifiedPhysicalTableName);
  }

  public org.apache.hadoop.hbase.TableName get() {
    return this.tablename;
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
      String physicalTableName) {

    if (physicalTableName == null || physicalTableName.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedPhysicalTableName");

    StringBuilder maprDBTableName = new StringBuilder();
    if (qualifiedPhysicalTableNamespace != null
        && qualifiedPhysicalTableNamespace.trim().length() > 0) {
      maprDBTableName.append(qualifiedPhysicalTableNamespace);
      maprDBTableName.append(MaprDBTableName.PHYSICAL_NAME_DELIMITER);
    }
    maprDBTableName.append(physicalTableName);

    return new MaprDBTableName("", maprDBTableName.toString());
  }

  public static MaprDBTableName fromLogicalName(String qualifiedLogicalTableNamespace,
      String logicalTableName) {
    if (qualifiedLogicalTableNamespace == null
        || qualifiedLogicalTableNamespace.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedLogicalTableNamespace");
    if (logicalTableName == null || logicalTableName.trim().length() == 0)
      throw new IllegalArgumentException("expected logicalTableName");
    StringBuilder maprDBTableName = new StringBuilder();
    String qualifiedPhysicalTableNamespace = qualifiedLogicalTableNamespace.replaceAll(
        TableMapping.TABLE_LOGICAL_NAME_DELIM, MaprDBTableName.PHYSICAL_NAME_DELIMITER);
    maprDBTableName.append(qualifiedPhysicalTableNamespace);
    maprDBTableName.append(MaprDBTableName.PHYSICAL_NAME_DELIMITER);
    maprDBTableName.append(logicalTableName);
    return new MaprDBTableName("", maprDBTableName.toString());
  }

  @Override
  public String getQualifiedLogicalName(StoreMappingContext mappingContext) {
    StringBuilder result = new StringBuilder();
    String volumePath = getTableName();
    // namespace has a prefix, strip it and its delim
    // to reconstruct just the logical name
    // which will necessarily not involve the root prefix
    if (mappingContext.hasTableNamespaceRoot()) {
      if (volumePath.startsWith(mappingContext.getTableNamespaceRoot())) {
        volumePath = volumePath.substring(mappingContext.getTableNamespaceRoot().length() + 1/*
                                                                                              * the
                                                                                              * delim
                                                                                              */);
      }
    }
    String qualifiedNamespace = volumePath;

    // replace physical with logical delimiters
    qualifiedNamespace = qualifiedNamespace.replaceAll(MaprDBTableName.PHYSICAL_NAME_DELIMITER,
        TableMapping.TABLE_LOGICAL_NAME_DELIM);
    result.append(qualifiedNamespace);
    result.append(TableMapping.TABLE_LOGICAL_NAME_DELIM);
    result.append(getTableName());
    return result.toString();
  }
}
