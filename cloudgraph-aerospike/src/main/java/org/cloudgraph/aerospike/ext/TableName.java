package org.cloudgraph.aerospike.ext;

public interface TableName {
  public String getNamespace();

  public String getTableName();

  public static TableName valueOf(String qualifiedPhysicalTableNamespace,
      String qualifiedPhysicalTableName) {
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
