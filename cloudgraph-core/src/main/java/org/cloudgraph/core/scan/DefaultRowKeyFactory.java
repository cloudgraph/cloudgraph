package org.cloudgraph.core.scan;

import org.cloudgraph.core.ServiceContext;
import org.plasma.sdo.PlasmaType;

public class DefaultRowKeyFactory implements RowKeyFactory {
  @Override
  public CompleteRowKey createCompleteRowKey(PlasmaType rootType, ServiceContext serviceContext) {
    CompleteRowKeyAssembler assembler = new CompleteRowKeyAssembler(rootType,
        serviceContext.getStoreMapping());
    assembler.assemble();
    return assembler;
  }

  @Override
  public PartialRowKey createPartialRowKey(PlasmaType rootType, ServiceContext serviceContext) {
    PartialRowKeyAssembler assembler = new PartialRowKeyAssembler(rootType,
        serviceContext.getStoreMapping());
    assembler.assemble();
    return assembler;
  }

  @Override
  public FuzzyRowKey createFuzzyRowKey(PlasmaType rootType, ServiceContext serviceContext) {
    FuzzyRowKeyAssembler assembler = new FuzzyRowKeyAssembler(rootType, serviceContext);
    assembler.assemble();
    return assembler;
  }

}
