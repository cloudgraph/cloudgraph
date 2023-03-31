package org.cloudgraph.core.client;

import java.io.IOException;
import java.util.List;

public interface Table {

  Result get(Get existing) throws IOException;

  void batch(List<Row> rows, Object[] results) throws InterruptedException, IOException;

  String getName();

  boolean checkAndMutate(byte[] row, byte[] checkFamBytes, byte[] checkQualBytes, CompareOper oper,
      byte[] checkValueBytes, RowMutations checkedMutations) throws IOException;

  Result[] get(List<Get> gets) throws IOException;

  ResultScanner getScanner(Scan scan) throws IOException;

  Object getConfiguration();

}
