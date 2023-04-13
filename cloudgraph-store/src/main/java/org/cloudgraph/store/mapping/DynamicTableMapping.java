package org.cloudgraph.store.mapping;

public class DynamicTableMapping extends TableMapping {

  private StoreMappingContext context;

  public DynamicTableMapping(Table table, MappingConfiguration config, StoreMappingContext context) {
    super(table, config);
    this.context = context;
  }

  public DynamicTableMapping(Table table, StoreMappingContext context) {
    super(table);
    this.context = context;
  }

  @Override
  public String getQualifiedLogicalName() {
    if (this.qualifiedLogicalName == null) {
      this.qualifiedLogicalName = qualifiedLogicalNameFor(this.table, this.context);
    }

    return this.qualifiedLogicalName;
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return this.context;
  }
}
