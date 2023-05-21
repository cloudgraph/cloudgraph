package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.TableName;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

public class HBaseTableName implements TableName {
  private org.apache.hadoop.hbase.TableName tablename;
  // Note: use the logical namespace delimiter
  public static String PHYSICAL_NAME_DELIMITER = "_";

  private HBaseTableName(String qualifiedPhysicalTableNamespace, String qualifiedPhysicalTableName) {
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
    HBaseTableName other = (HBaseTableName) obj;
    if (tablename == null) {
      if (other.tablename != null)
        return false;
    } else if (!tablename.equals(other.tablename))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return this.tablename.toString();
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
    qualifiedNamespace = qualifiedNamespace.replaceAll(HBaseTableName.PHYSICAL_NAME_DELIMITER,
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
