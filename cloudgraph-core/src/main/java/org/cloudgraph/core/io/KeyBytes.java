package org.cloudgraph.core.io;

import java.util.Arrays;

public class KeyBytes {
  private byte[] key;
  private int hashCode;

  public KeyBytes(byte[] key) {
    super();
    this.key = key;
    this.hashCode = Arrays.hashCode(this.key);
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    KeyBytes other = (KeyBytes) obj;
    if (hashCode != other.hashCode)
      return false;
    return true;
  }

  public byte[] getKey() {
    return key;
  }

}