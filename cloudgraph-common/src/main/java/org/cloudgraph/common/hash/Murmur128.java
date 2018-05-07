package org.cloudgraph.common.hash;

import java.util.HashSet;
import java.util.Set;

import org.apache.jena.ext.com.google.common.hash.HashCode;
import org.apache.jena.ext.com.google.common.hash.HashFunction;
import org.apache.jena.ext.com.google.common.hash.Hashing;

public class Murmur128 implements Hash64 {
  private static volatile Murmur128 INSTANCE;

  public static Murmur128 instance() {
    if (INSTANCE == null)
      initInstance();
    return INSTANCE;
  }

  private static synchronized void initInstance() {
    if (INSTANCE == null)
      INSTANCE = new Murmur128();
  }

  @Override
  public long hash(byte[] bytes) {
    HashFunction hf = Hashing.murmur3_128(-1);
    HashCode hc = hf.newHasher().putBytes(bytes).hash();
    return hc.asLong();
  }

  @Override
  public long hash(byte[] bytes, int seed) {
    HashFunction hf = Hashing.murmur3_128(seed);
    HashCode hc = hf.newHasher().putBytes(bytes).hash();
    return hc.asLong();
  }

  public static byte[] toBytes(long val) {
    byte[] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  public static void main(String[] args) {
    Murmur128 hash = new Murmur128();
    Set<Long> map = new HashSet<Long>();

    for (long i = 1000000000; i < 10000000000L; i++) {
      if (map.contains(i))
        throw new RuntimeException("collision: " + i);
      map.add(i);
      if (i % 1000 == 0)
        System.out.println(i + ": " + String.valueOf(hash.hash(toBytes(i))));
    }
  }

}