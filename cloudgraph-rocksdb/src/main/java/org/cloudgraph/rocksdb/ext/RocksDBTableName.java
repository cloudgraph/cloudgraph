package org.cloudgraph.rocksdb.ext;

import org.cloudgraph.core.client.TableName;
import org.cloudgraph.store.mapping.StoreMappingContext;

public class RocksDBTableName implements TableName {
  private String qualifiedPhysicalTableNamespace;
  private String qualifiedPhysicalTableName;

  public RocksDBTableName(String qualifiedPhysicalTableNamespace, String qualifiedPhysicalTableName) {
    super();
    this.qualifiedPhysicalTableNamespace = qualifiedPhysicalTableNamespace;
    this.qualifiedPhysicalTableName = qualifiedPhysicalTableName;
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
