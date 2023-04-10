package org.cloudgraph.store.mapping;

public class DefaultTableMappingFactory implements TableMappingFactory {

  @Override
  public StaticTableMapping createStaticTableMapping(Table table, MappingConfiguration config) {
    return new StaticTableMapping(table, config);
  }

  @Override
  public StaticTableMapping createStaticTableMapping(Table table) {
    return new StaticTableMapping(table);
  }

  @Override
  public DynamicTableMapping createDynamicTableMapping(Table table, MappingConfiguration config,
      StoreMappingContext context) {

    return new DynamicTableMapping(table, config, context);
  }

  @Override
  public DynamicTableMapping createDynamicTableMapping(Table table, StoreMappingContext context) {
    return new DynamicTableMapping(table, context);
  }

}
