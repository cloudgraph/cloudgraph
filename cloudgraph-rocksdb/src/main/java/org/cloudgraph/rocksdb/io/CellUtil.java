package org.cloudgraph.rocksdb.io;

import org.cloudgraph.core.client.Cell;
import org.cloudgraph.core.client.KeyValue;

public class CellUtil {

  public static byte[] cloneFamily(KeyValue keyValue) {
    return keyValue.getFamily();
  }

  public static byte[] cloneQualifier(KeyValue keyValue) {
    return keyValue.getQualifier();
  }

  public static byte[] cloneValue(Cell cell) {
    return cell.getValueArray();
  }

  public static byte[] cloneFamily(Cell cell) {
    return cell.getFamilyArray();
  }

  public static byte[] cloneQualifier(Cell cell) {
    return cell.getQualifierArray();
  }

  public static byte[] cloneValue(KeyValue keyValue) {
    return keyValue.getValue();
  }

}
