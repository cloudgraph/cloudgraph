package org.cloudgraph.rocksdb.filter;

import java.util.Set;

import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.filter.ColumnFilterFactory;
import org.cloudgraph.core.io.EdgeReader;
import org.cloudgraph.core.util.PredicateUtil;
import org.cloudgraph.rocksdb.ext.RocksDBFilter;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.collector.Selection;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

public class RocksDBColumnFilterFactory implements ColumnFilterFactory {

  @Override
  public Filter createGraphFetchColumnFilter(Selection selection, PlasmaType type,
      StoreMappingContext mappingContext) {
    GraphFetchColumnFilterAssembler columnFilterAssembler = new GraphFetchColumnFilterAssembler(
        selection, type, mappingContext);
    return new RocksDBFilter(columnFilterAssembler.getFilter());

  }

  @Override
  public Filter createColumnPredicateFilter(PlasmaType rootType, Where where,
      PlasmaType contextType, StoreMappingContext storeMapping) {
    PredicateUtil predicateUtil = new PredicateUtil();
    boolean multiDescendantProperties = predicateUtil.hasHeterogeneousDescendantProperties(where);
    // if (!multiDescendantProperties) {
    ColumnPredicateFilterAssembler filterAssembler = new ColumnPredicateFilterAssembler(rootType,
        storeMapping);
    // }
    // else {
    // filterAssembler = new
    // MultiColumnPredicateFilterAssembler(rowReader.getGraphState(),
    // rootType);
    // }
    filterAssembler.assemble(where, contextType);
    return new RocksDBFilter(filterAssembler.getFilter());

  }

  @Override
  public Filter createBinaryPrefixColumnFilter(PlasmaType rootType, Set<Property> properties,
      PlasmaType contextType, StoreMappingContext storeMapping) {
    BinaryPrefixColumnFilterAssembler columnFilterAssembler = new BinaryPrefixColumnFilterAssembler(
        rootType, storeMapping);

    columnFilterAssembler.assemble(properties, contextType);
    return new RocksDBFilter(columnFilterAssembler.getFilter());
  }

  @Override
  public Filter createStatefullBinaryPrefixColumnFilter(PlasmaType rootType,
      Set<Property> properties, Set<Long> sequences, PlasmaType contextType, EdgeReader edgeReader) {
    StatefullBinaryPrefixColumnFilterAssembler columnFilterAssembler = new StatefullBinaryPrefixColumnFilterAssembler(
        rootType, edgeReader);
    columnFilterAssembler.assemble(properties, sequences, contextType);
    return new RocksDBFilter(columnFilterAssembler.getFilter());
  }

  @Override
  public Filter createInitialFetchColumnFilter(SelectionCollector collector, PlasmaType type,
      StoreMappingContext storeMapping) {
    InitialFetchColumnFilterAssembler columnFilterAssembler = new InitialFetchColumnFilterAssembler(
        collector, type, storeMapping);
    return new RocksDBFilter(columnFilterAssembler.getFilter());
  }

}
