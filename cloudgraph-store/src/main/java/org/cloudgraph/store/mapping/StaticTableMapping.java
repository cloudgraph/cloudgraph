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
  public String getQualifiedName() {
    return qualifiedNameFor(this.getNamespace(), this.getName(), null);
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return null;
  }
}
