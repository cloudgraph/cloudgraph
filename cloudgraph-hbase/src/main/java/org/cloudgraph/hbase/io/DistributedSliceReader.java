package org.cloudgraph.hbase.io;

import java.util.List;

import commonj.sdo.Type;

public class DistributedSliceReader extends DistributedGraphReader {

  public DistributedSliceReader(Type rootType, List<Type> types, DistributedReader parentGraphReader) {
    super(rootType, types, parentGraphReader.getMarshallingContext(), parentGraphReader
        .getConnection());
  }

}
