package org.cloudgraph.aerospike.ext;

import com.aerospike.client.Key;

public class KeyInfo {
  private Key key;
  private String family;

  @SuppressWarnings("unused")
  private KeyInfo() {
  }

  public KeyInfo(Key key, String family) {
    super();
    this.key = key;
    this.family = family;
  }

  public Key getKey() {
    return key;
  }

  public String getFamily() {
    return family;
  }

}
