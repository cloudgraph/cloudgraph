package org.cloudgraph.core.client;

public interface CellUtil {

  byte[] cloneValue(Cell last);

  byte[] cloneQualifier(Cell cell);

  byte[] cloneFamily(Cell cell);

}
