package org.cloudgraph.rocksdb.client;

import org.cloudgraph.core.client.TableName;
import org.cloudgraph.store.mapping.StoreMappingContext;

public class RocksDBTableName implements TableName {
  public static final String PHYSICAL_NAME_DELIMITER = "-";
  private String qualifiedPhysicalTableNamespace;
  private String qualifiedPhysicalTableName;

  public RocksDBTableName(String qualifiedPhysicalTableNamespace, String qualifiedPhysicalTableName) {
    super();
    this.qualifiedPhysicalTableNamespace = qualifiedPhysicalTableNamespace;
    this.qualifiedPhysicalTableName = qualifiedPhysicalTableName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((qualifiedPhysicalTableName == null) ? 0 : qualifiedPhysicalTableName.hashCode());
    result = prime
        * result
        + ((qualifiedPhysicalTableNamespace == null) ? 0 : qualifiedPhysicalTableNamespace
            .hashCode());
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
    RocksDBTableName other = (RocksDBTableName) obj;
    if (qualifiedPhysicalTableName == null) {
      if (other.qualifiedPhysicalTableName != null)
        return false;
    } else if (!qualifiedPhysicalTableName.equals(other.qualifiedPhysicalTableName))
      return false;
    if (qualifiedPhysicalTableNamespace == null) {
      if (other.qualifiedPhysicalTableNamespace != null)
        return false;
    } else if (!qualifiedPhysicalTableNamespace.equals(other.qualifiedPhysicalTableNamespace))
      return false;
    return true;
  }

  @Override
  public String getNamespace() {
    return qualifiedPhysicalTableNamespace;
  }

  @Override
  public String getTableName() {
    return qualifiedPhysicalTableName;
  }

  public static RocksDBTableName valueOf(String qualifiedPhysicalTableNamespace,
      String qualifiedPhysicalTableName) {
    if (qualifiedPhysicalTableNamespace == null
        || qualifiedPhysicalTableNamespace.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedPhysicalTableNamespace");
    if (qualifiedPhysicalTableName == null || qualifiedPhysicalTableName.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedPhysicalTableName");
    return new RocksDBTableName(qualifiedPhysicalTableNamespace, qualifiedPhysicalTableName);
  }

  @Override
  public String getQualifiedLogicalName(StoreMappingContext mappingContext) {
    // TODO Auto-generated method stub
    return null;
  }

}
