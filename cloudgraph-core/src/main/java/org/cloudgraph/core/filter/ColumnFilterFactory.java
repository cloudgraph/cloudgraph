package org.cloudgraph.core.filter;

import java.util.Set;

import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.io.EdgeReader;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.collector.Selection;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.query.model.Where;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

public interface ColumnFilterFactory {

  Filter createGraphFetchColumnFilter(Selection selection, PlasmaType type,
      StoreMappingContext mappingContext);

  Filter createColumnPredicateFilter(PlasmaType rootType, Where where, PlasmaType contextType,
      StoreMappingContext storeMapping);

  Filter createBinaryPrefixColumnFilter(PlasmaType rootType, Set<Property> properties,
      PlasmaType contextType, StoreMappingContext storeMapping);

  Filter createStatefullBinaryPrefixColumnFilter(PlasmaType rootType, Set<Property> properties,
      Set<Long> sequences, PlasmaType contextType, EdgeReader edgeReader);

  Filter createInitialFetchColumnFilter(SelectionCollector collector, PlasmaType type,
      StoreMappingContext storeMapping);

}
