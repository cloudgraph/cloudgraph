package org.cloudgraph.common.hash;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 *
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */
public class MurmurHash implements Hash {
  private static volatile MurmurHash INSTANCE;

  public static MurmurHash instance() {
    if (INSTANCE == null)
      initInstance();
    return INSTANCE;
  }

  private static synchronized void initInstance() {
    if (INSTANCE == null)
      INSTANCE = new MurmurHash();
  }

  @Override
  public int hash(byte[] bytes) {
    return hash(bytes, bytes.length, -1);
  }

  @Override
  public int hash(byte[] bytes, int initval) {
    return hash(bytes, 0, bytes.length, initval);
  }

  @Override
  public int hash(byte[] data, int offset, int length) {
    return hash(data, 0, length, -1);
  }

  public int hash(byte[] data, int offset, int length, int seed) {
    int m = 0x5bd1e995;
    int r = 24;

    int h = seed ^ length;

    int len_4 = length >> 2;

    for (int i = 0; i < len_4; i++) {
      int i_4 = (i << 2) + offset;
      int k = data[i_4 + 3];
      k = k << 8;
      k = k | (data[i_4 + 2] & 0xff);
      k = k << 8;
      k = k | (data[i_4 + 1] & 0xff);
      k = k << 8;
      // noinspection PointlessArithmeticExpression
      k = k | (data[i_4 + 0] & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // avoid calculating modulo
    int len_m = len_4 << 2;
    int left = length - len_m;
    int i_m = len_m + offset;

    if (left != 0) {
      if (left >= 3) {
        h ^= data[i_m + 2] << 16;
      }
      if (left >= 2) {
        h ^= data[i_m + 1] << 8;
      }
      if (left >= 1) {
        h ^= data[i_m];
      }

      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }
}
