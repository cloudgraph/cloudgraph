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
  public String getPhysicalName() {
    if (this.physicalName == null) {
      this.physicalName = physicalNameFor(this.table.getName(), this.context);
    }

    return this.physicalName;
  }

  @Override
  public String getNamespaceQualifiedPhysicalName() {
    if (this.namespaceQualifiedPhysicalName == null) {
      this.namespaceQualifiedPhysicalName = namespaceQualifiedPhysicalNameFor(
          this.table.getNamespace(), this.table.getName(), this.context);
    }

    return this.namespaceQualifiedPhysicalName;
  }

  @Override
  public String getQualifiedPhysicalNamespace() {
    if (this.qualifiedPhysicalNamespace == null) {
      this.qualifiedPhysicalNamespace = qualifiedPhysicalNamespaceFor(this.table, this.context);
    }

    return this.qualifiedPhysicalNamespace;
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return this.context;
  }
}
