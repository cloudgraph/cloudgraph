package org.cloudgraph.common.hash;

public interface Hash {

  /**
   * Calculate a hash using all bytes from the input argument, and a seed of -1.
   * Uses the hash algorithm defined for this specific HBase table or if no
   * defined, uses the hash algorithm set for the HBase configuration.
   * 
   * @param bytes
   * @return the hash value
   */
  public abstract int hash(byte[] bytes);

  /**
   * Calculate a hash using bytes from 0 to length, and the provided seed value.
   * Uses the hash algorithm defined for this specific HBase table or if no
   * defined, uses the hash algorithm set for the HBase configuration.
   * 
   * @param bytes
   * @param length
   * @param initval
   * @return the hash value
   */
  public abstract int hash(byte[] bytes, int initval);

  public abstract int hash(byte[] data, int offset, int length);

}