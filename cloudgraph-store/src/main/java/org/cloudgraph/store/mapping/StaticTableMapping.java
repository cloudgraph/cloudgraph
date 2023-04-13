package org.cloudgraph.store.mapping;

public class StaticTableMapping extends TableMapping {

  public StaticTableMapping(Table table, MappingConfiguration config) {
    super(table, config);
  }

  public StaticTableMapping(Table table) {
    super(table);
  }

  @Override
  public String getQualifiedLogicalName() {
    if (this.qualifiedLogicalName == null) {
      this.qualifiedLogicalName = qualifiedLogicalNameFor(this.table, null); // static
                                                                             // mapping
                                                                             // w/o
                                                                             // context
    }

    return this.qualifiedLogicalName;
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return null;
  }
}
