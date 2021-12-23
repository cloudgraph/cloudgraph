/**
 * Copyright 2017 TerraMeta Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudgraph.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class Bytes {
  private static final String UTF8 = "UTF-8";
  public static final Charset UTF8_CHARSET = Charset.forName(UTF8);
  /**
   * Size of boolean in bytes
   */
  public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

  /**
   * Size of byte in bytes
   */
  public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

  /**
   * Size of char in bytes
   */
  public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

  /**
   * Size of double in bytes
   */
  public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

  /**
   * Size of float in bytes
   */
  public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

  /**
   * Size of int in bytes
   */
  public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  /**
   * Size of long in bytes
   */
  public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  /**
   * Size of short in bytes
   */
  public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

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

  public static String toString(final byte[] b) {
    if (b == null) {
      return null;
    }
    return toString(b, 0, b.length);
  }

  /**
   * Joins two byte arrays together using a separator.
   * 
   * @param b1
   *          The first byte array.
   * @param sep
   *          The separator to use.
   * @param b2
   *          The second byte array.
   */
  public static String toString(final byte[] b1, String sep, final byte[] b2) {
    return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
  }

  /**
   * This method will convert utf8 encoded bytes into a string. If the given
   * byte array is null, this method will return null.
   * 
   * @param b
   *          Presumed UTF-8 encoded byte array.
   * @param off
   *          offset into array
   * @return String made from <code>b</code> or null
   */
  public static String toString(final byte[] b, int off) {
    if (b == null) {
      return null;
    }
    int len = b.length - off;
    if (len <= 0) {
      return "";
    }
    return new String(b, off, len, UTF8_CHARSET);
  }

  /**
   * This method will convert utf8 encoded bytes into a string. If the given
   * byte array is null, this method will return null.
   *
   * @param b
   *          Presumed UTF-8 encoded byte array.
   * @param off
   *          offset into array
   * @param len
   *          length of utf-8 sequence
   * @return String made from <code>b</code> or null
   */
  public static String toString(final byte[] b, int off, int len) {
    if (b == null) {
      return null;
    }
    if (len == 0) {
      return "";
    }
    return new String(b, off, len, UTF8_CHARSET);
  }

  /**
   * Converts a string to a UTF-8 byte array.
   * 
   * @param s
   *          string
   * @return the byte array
   */
  public static byte[] toBytes(String s) {
    return s.getBytes(UTF8_CHARSET);
  }

  /**
   * Convert a boolean to a byte array. True becomes -1 and false becomes 0.
   *
   * @param b
   *          value
   * @return <code>b</code> encoded in a byte array.
   */
  public static byte[] toBytes(final boolean b) {
    return new byte[] { b ? (byte) -1 : (byte) 0 };
  }

  /**
   * Reverses {@link #toBytes(boolean)}
   * 
   * @param b
   *          array
   * @return True or false.
   */
  public static boolean toBoolean(final byte[] b) {
    if (b.length != 1) {
      throw new IllegalArgumentException("Array has wrong size: " + b.length);
    }
    return b[0] != (byte) 0;
  }

  /**
   * Convert a long value to a byte array using big-endian.
   *
   * @param val
   *          value to convert
   * @return the byte array
   */
  public static byte[] toBytes(long val) {
    byte[] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

}
