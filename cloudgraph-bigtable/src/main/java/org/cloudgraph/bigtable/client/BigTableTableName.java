package org.cloudgraph.bigtable.client;

import org.cloudgraph.core.client.TableName;
import org.cloudgraph.hbase.client.HBaseTableName;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

public class BigTableTableName implements TableName {
  private org.apache.hadoop.hbase.TableName tablename;
  public static String PHYSICAL_NAME_DELIMITER = ".";
  public static String PHYSICAL_NAME_DELIMITER_REGEX = "\\.";

  private BigTableTableName(String qualifiedPhysicalTableNamespace,
      String qualifiedPhysicalTableName) {
    this.tablename = org.apache.hadoop.hbase.TableName.valueOf(qualifiedPhysicalTableNamespace,
        qualifiedPhysicalTableName);
  }

  public org.apache.hadoop.hbase.TableName get() {
    return this.tablename;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((tablename == null) ? 0 : tablename.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BigTableTableName other = (BigTableTableName) obj;
    if (tablename == null) {
      if (other.tablename != null)
        return false;
    } else if (!tablename.equals(other.tablename))
      return false;
    return true;
  }

  @Override
  public String getQualifiedLogicalName(StoreMappingContext mappingContext) {
    StringBuilder result = new StringBuilder();
    if (mappingContext.hasTableNamespaceRoot()) {
      throw new IllegalStateException("unexpected namespace root: "
          + mappingContext.getTableNamespaceRoot());
    }

    String qualifiedLogicalName = getTableName();

    // replace physical with logical delimiters
    qualifiedLogicalName = qualifiedLogicalName.replaceAll(
        BigTableTableName.PHYSICAL_NAME_DELIMITER_REGEX, TableMapping.TABLE_LOGICAL_NAME_DELIM);
    result.append(qualifiedLogicalName);
    return result.toString();
  }

  @Override
  public String getNamespace() {
    // will be empty for bigtable as table namespaces
    // not supported as such, the instance name becoming
    // effectively the namespace
    return this.tablename.getNamespaceAsString();
  }

  @Override
  public String getTableName() {
    // Note: in HBase the getTableAsString() returns the namespace qualified
    // name as 'namespace:name'
    // and we are needing just the "qualifier"
    return this.tablename.getQualifierAsString();
  }

  public static BigTableTableName valueOf(String qualifiedPhysicalTableNamespace,
      String physicalTableName) {
    if (qualifiedPhysicalTableNamespace == null
        || qualifiedPhysicalTableNamespace.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedPhysicalTableNamespace");
    if (physicalTableName == null || physicalTableName.trim().length() == 0)
      throw new IllegalArgumentException("expected physicalTableName");
    StringBuilder result = new StringBuilder();
    result.append(qualifiedPhysicalTableNamespace);
    result.append(BigTableTableName.PHYSICAL_NAME_DELIMITER);
    result.append(physicalTableName);
    return new BigTableTableName("", result.toString());
  }

  public static BigTableTableName fromLogicalName(String qualifiedLogicalTableNamespace,
      String logicalTableName, StoreMappingContext mappingContext) {
    if (qualifiedLogicalTableNamespace == null
        || qualifiedLogicalTableNamespace.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedLogicalTableNamespace");
    if (logicalTableName == null || logicalTableName.trim().length() == 0)
      throw new IllegalArgumentException("expected logicalTableName");
    StringBuilder bigTableTableName = new StringBuilder();
    String qualifiedPhysicalTableNamespace = qualifiedLogicalTableNamespace.replaceAll(
        TableMapping.TABLE_LOGICAL_NAME_DELIM, BigTableTableName.PHYSICAL_NAME_DELIMITER);
    if (mappingContext.hasTableNamespaceRoot()) {
      bigTableTableName.append(mappingContext.getTableNamespaceRoot());
      bigTableTableName.append(BigTableTableName.PHYSICAL_NAME_DELIMITER);
    }
    bigTableTableName.append(qualifiedPhysicalTableNamespace);
    bigTableTableName.append(BigTableTableName.PHYSICAL_NAME_DELIMITER);
    bigTableTableName.append(logicalTableName);
    return new BigTableTableName("", bigTableTableName.toString());
  }

}
