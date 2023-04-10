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
  public String getPhysicalName() {
    if (this.physicalName == null) {
      this.physicalName = physicalNameFor(this.table.getName(), null); // static
                                                                       // mapping
                                                                       // w/o
                                                                       // context
    }

    return this.physicalName;
  }

  @Override
  public String getNamespaceQualifiedPhysicalName() {
    if (this.namespaceQualifiedPhysicalName == null) {
      this.namespaceQualifiedPhysicalName = namespaceQualifiedPhysicalNameFor(
          this.table.getNamespace(), this.table.getName(), null); // static
                                                                  // mapping w/o
                                                                  // context
    }

    return this.namespaceQualifiedPhysicalName;
  }

  @Override
  public String getQualifiedPhysicalNamespace() {
    if (this.qualifiedPhysicalNamespace == null) {
      this.qualifiedPhysicalNamespace = qualifiedPhysicalNamespaceFor(this.table, null); // static
                                                                                         // mapping
                                                                                         // w/o
                                                                                         // context
    }

    return this.qualifiedPhysicalNamespace;
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return null;
  }
}
