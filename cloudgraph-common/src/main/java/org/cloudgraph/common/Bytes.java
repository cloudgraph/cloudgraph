package org.cloudgraph.common;

public class Bytes {
  public static byte[] concat(byte[]... fields) {
    int i = 0;
    for (byte[] field : fields)
      i += field.length;
    byte[] result = new byte[i];
    i = 0;
    for (byte[] field : fields) {
      System.arraycopy(field, 0, result, i, field.length);
      i += field.length;
    }
    return result;
  }

}
