package org.cloudgraph.rocksdb.ext;

public interface TableName {
  public String getNamespace();

  public String getTableName();

  public static TableName valueOf(String qualifiedPhysicalTableNamespace,
      String qualifiedPhysicalTableName) {
    if (qualifiedPhysicalTableNamespace == null
        || qualifiedPhysicalTableNamespace.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedPhysicalTableNamespace");
    if (qualifiedPhysicalTableName == null || qualifiedPhysicalTableName.trim().length() == 0)
      throw new IllegalArgumentException("expected qualifiedPhysicalTableName");
    return new TableName() {

      @Override
      public String getNamespace() {
        return qualifiedPhysicalTableNamespace;
      }

      @Override
      public String getTableName() {
        return qualifiedPhysicalTableName;
      }
    };
  }

}
