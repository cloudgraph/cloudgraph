package org.cloudgraph.rocksdb.client;

import org.cloudgraph.common.Bytes;

public class Key {
  private String namespace;
  private String tableName;
  private byte[] value;

  public Key(byte[] data) {
    super();
    this.value = data;
  }

  public Key(String namespace, String tableName, byte[] value) {
    this.namespace = namespace;
    this.tableName = tableName;
    this.value = value;
  }

  public byte[] getData() {
    return value;
  }

  @Override
  public String toString() {
    String keyString = Bytes.toString(value);
    keyString = keyString.replace(" ", "");
    return "Key [namespace=" + namespace + ", tableName=" + tableName + ", value=" + keyString
        + "]";
  }

}
