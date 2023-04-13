package org.cloudgraph.core.client;

 import org.cloudgraph.store.mapping.StoreMappingContext;

public interface TableName {

  /**
   * returns the physical , store specific, table namespace
   * 
   * @return the physical , store specific, table namespace
   */
  public String getNamespace();

  /**
   * returns the physical , store specific, table name
   * 
   * @return the physical , store specific, table name
   */
  public String getTableName();

  /**
   * Constructs a logical table name from a physical, store specific, table name
   * 
   * @param mappingContext
   *          the mapping context
   * @return the logical table name
   */
  public String getQualifiedLogicalName(StoreMappingContext mappingContext);

 
}
