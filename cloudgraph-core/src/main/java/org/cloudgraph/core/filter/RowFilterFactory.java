package org.cloudgraph.core.filter;

import java.util.List;

import org.cloudgraph.core.client.Filter;
import org.cloudgraph.core.scan.FuzzyRowKey;
import org.cloudgraph.core.scan.PartialRowKey;

public interface RowFilterFactory {

  Filter createFuzzyRowFilter(FuzzyRowKey fuzzyScan);

  Filter createRandomRowFilter(Float sample, Filter columnFilter);

}
