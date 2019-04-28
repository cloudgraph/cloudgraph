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
      this.qualifiedLogicalName = qualifiedLogicalNameFor(this.table.getNamespace(),
          this.table.getName(), this.context);
    }

    return this.qualifiedLogicalName;
  }

  @Override
  public String getQualifiedPhysicalName() {
    if (this.qualifiedPhysicalName == null) {
      this.qualifiedPhysicalName = qualifiedPhysicalNameFor(this.table.getNamespace(),
          this.table.getName(), this.context);
    }

    return this.qualifiedPhysicalName;
  }

  @Override
  public String getRelativePhysicalName() {
    if (this.relativePhysicalName == null) {
      this.relativePhysicalName = relativePhysicalNameFor(this.table.getNamespace(),
          this.table.getName(), this.context);
    }

    return this.relativePhysicalName;
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return this.context;
  }
}
