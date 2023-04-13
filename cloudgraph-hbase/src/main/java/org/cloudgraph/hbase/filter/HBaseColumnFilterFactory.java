package org.cloudgraph.hbase.filter;

import java.util.Set;

import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.filter.ColumnFilterFactory;
import org.cloudgraph.core.io.EdgeReader;
import org.cloudgraph.core.util.PredicateUtil;
import org.cloudgraph.hbase.client.HBaseFilter;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.collector.Selection;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

public class HBaseColumnFilterFactory implements ColumnFilterFactory {

  @Override
  public Filter createGraphFetchColumnFilter(Selection selection, PlasmaType type,
      ServiceContext serviceContext) {
    GraphFetchColumnFilterAssembler columnFilterAssembler = new GraphFetchColumnFilterAssembler(
        selection, type, serviceContext);
    return new HBaseFilter(columnFilterAssembler.getFilter());

  }

  @Override
  public Filter createColumnPredicateFilter(PlasmaType rootType, Where where,
      PlasmaType contextType, ServiceContext serviceContext) {
    PredicateUtil predicateUtil = new PredicateUtil();
    boolean multiDescendantProperties = predicateUtil.hasHeterogeneousDescendantProperties(where);
    // if (!multiDescendantProperties) {
    ColumnPredicateFilterAssembler filterAssembler = new ColumnPredicateFilterAssembler(rootType,
        serviceContext);
    // }
    // else {
    // filterAssembler = new
    // MultiColumnPredicateFilterAssembler(rowReader.getGraphState(),
    // rootType);
    // }
    filterAssembler.assemble(where, contextType);
    return new HBaseFilter(filterAssembler.getFilter());

  }

  @Override
  public Filter createBinaryPrefixColumnFilter(PlasmaType rootType, Set<Property> properties,
      PlasmaType contextType, ServiceContext serviceContext) {
    BinaryPrefixColumnFilterAssembler columnFilterAssembler = new BinaryPrefixColumnFilterAssembler(
        rootType, serviceContext);

    columnFilterAssembler.assemble(properties, contextType);
    return new HBaseFilter(columnFilterAssembler.getFilter());
  }

  @Override
  public Filter createStatefullBinaryPrefixColumnFilter(PlasmaType rootType,
      Set<Property> properties, Set<Long> sequences, PlasmaType contextType, EdgeReader edgeReader) {
    StatefullBinaryPrefixColumnFilterAssembler columnFilterAssembler = new StatefullBinaryPrefixColumnFilterAssembler(
        rootType, edgeReader);
    columnFilterAssembler.assemble(properties, sequences, contextType);
    return new HBaseFilter(columnFilterAssembler.getFilter());
  }

  @Override
  public Filter createInitialFetchColumnFilter(SelectionCollector collector, PlasmaType type,
      ServiceContext serviceContext) {
    InitialFetchColumnFilterAssembler columnFilterAssembler = new InitialFetchColumnFilterAssembler(
        collector, type, serviceContext);
    return new HBaseFilter(columnFilterAssembler.getFilter());
  }

}
