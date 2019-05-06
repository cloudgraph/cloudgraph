package org.cloudgraph.store.mapping;

import java.nio.charset.Charset;
import java.util.List;

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
      this.qualifiedLogicalName = qualifiedLogicalNameFor(this.table.getNamespace(),
          this.table.getName(), null); // static mapping w/o context
    }

    return this.qualifiedLogicalName;
  }

  @Override
  public String getQualifiedPhysicalName() {
    if (this.qualifiedPhysicalName == null) {
      this.qualifiedPhysicalName = qualifiedPhysicalNameFor(this.table.getNamespace(),
          this.table.getName(), null); // static mapping w/o context
    }

    return this.qualifiedPhysicalName;
  }

  @Override
  public String getRelativePhysicalName() {
    if (this.relativePhysicalName == null) {
      this.relativePhysicalName = relativePhysicalNameFor(this.table.getNamespace(),
          this.table.getName(), null); // static mapping w/o context
    }

    return this.relativePhysicalName;
  }

  @Override
  public String getQualifiedPhysicalNamespace() {
    if (this.qualifiedPhysicalNamespace == null) {
      this.qualifiedPhysicalNamespace = qualifiedPhysicalNamespaceFor(this.table.getNamespace(),
          this.table.getName(), null); // static mapping w/o context
    }

    return this.qualifiedPhysicalNamespace;
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return null;
  }
}
