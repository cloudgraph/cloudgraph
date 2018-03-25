package org.cloudgraph.common.hash;

/**
 * Uses at least a 64 bit hash algorithm and returns a 64 bit long integer hash
 * 
 * @author Scott Cinnamond
 */
public interface Hash64 {
  /**
   * Calculate at least a 64 bit hash using all bytes from the input argument,
   * and a seed of -1.
   * 
   * @param bytes
   * @return the hash value
   */
  public abstract long hash(byte[] bytes);

  /**
   * Calculate a hash using bytes from 0 to length, and the provided seed value.
   * 
   * @param bytes
   * @param length
   * @param seed
   * @return the hash value
   */
  public abstract long hash(byte[] bytes, int seed);

}
