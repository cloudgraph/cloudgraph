package org.cloudgraph.store.mapping;

public interface TableMappingFactory {
  public StaticTableMapping createStaticTableMapping(Table table, MappingConfiguration config);

  public StaticTableMapping createStaticTableMapping(Table table);

  public DynamicTableMapping createDynamicTableMapping(Table table, MappingConfiguration config,
      StoreMappingContext context);

  public DynamicTableMapping createDynamicTableMapping(Table table, StoreMappingContext context);
}
