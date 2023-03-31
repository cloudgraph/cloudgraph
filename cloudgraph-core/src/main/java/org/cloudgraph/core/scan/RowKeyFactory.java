package org.cloudgraph.core.scan;

import org.cloudgraph.core.ServiceContext;
import org.plasma.sdo.PlasmaType;

public interface RowKeyFactory {
  public CompleteRowKey createCompleteRowKey(PlasmaType rootType, ServiceContext serviceContext);

  public PartialRowKey createPartialRowKey(PlasmaType rootType, ServiceContext serviceContext);

  public FuzzyRowKey createFuzzyRowKey(PlasmaType rootType, ServiceContext serviceContext);

}
