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

  public static float toFloat(byte[] bytes) {
    return toFloat(bytes, 0);
  }

  public static float toFloat(byte[] bytes, int offset) {
    return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
  }

  public static double toDouble(final byte[] bytes) {
    return toDouble(bytes, 0);
  }

  /**
   * @param bytes
   *          byte array
   * @param offset
   *          offset where double is
   * @return Return double made from passed bytes.
   */
  public static double toDouble(final byte[] bytes, final int offset) {
    return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
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

  public static byte[] toBytes(BigDecimal val) {
    byte[] valueBytes = val.unscaledValue().toByteArray();
    byte[] result = new byte[valueBytes.length + SIZEOF_INT];
    int offset = putInt(result, 0, val.scale());
    putBytes(result, offset, valueBytes, 0, valueBytes.length);
    return result;
  }

  public static int putInt(byte[] bytes, int offset, int val) {
    if (bytes.length - offset < SIZEOF_INT) {
      throw new IllegalArgumentException("Not enough room to put an int at" + " offset " + offset
          + " in a " + bytes.length + " byte array");
    }
    for (int i = offset + 3; i > offset; i--) {
      bytes[i] = (byte) val;
      val >>>= 8;
    }
    bytes[offset] = (byte) val;
    return offset + SIZEOF_INT;
  }

  /**
   * Put bytes at the specified byte array position.
   * 
   * @param tgtBytes
   *          the byte array
   * @param tgtOffset
   *          position in the array
   * @param srcBytes
   *          array to write out
   * @param srcOffset
   *          source offset
   * @param srcLength
   *          source length
   * @return incremented offset
   */
  public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes, int srcOffset,
      int srcLength) {
    System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
    return tgtOffset + srcLength;
  }

  /**
   * Write a single byte out to the specified byte array position.
   * 
   * @param bytes
   *          the byte array
   * @param offset
   *          position in the array
   * @param b
   *          byte to write out
   * @return incremented offset
   */
  public static int putByte(byte[] bytes, int offset, byte b) {
    bytes[offset] = b;
    return offset + 1;
  }

  public static long toLong(byte[] bytes) {
    return toLong(bytes, 0, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value. Assumes there will be
   * {@link #SIZEOF_LONG} bytes available.
   *
   * @param bytes
   *          bytes
   * @param offset
   *          offset
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    return toLong(bytes, offset, SIZEOF_LONG);
  }

  public static long toLong(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_LONG || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
    }
    long l = 0;
    for (int i = offset; i < offset + length; i++) {
      l <<= 8;
      l ^= bytes[i] & 0xFF;
    }
    return l;
  }

  public static short toShort(byte[] bytes) {
    return toShort(bytes, 0, SIZEOF_SHORT);
  }

  public static short toShort(byte[] bytes, int offset) {
    return toShort(bytes, offset, SIZEOF_SHORT);
  }

  public static short toShort(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_SHORT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
    }
    short n = 0;
    n = (short) ((n ^ bytes[offset]) & 0xFF);
    n = (short) (n << 8);
    n = (short) ((n ^ bytes[offset + 1]) & 0xFF);
    return n;
  }

  public static byte[] toBytes(final float f) {
    // Encode it as int
    return Bytes.toBytes(Float.floatToRawIntBits(f));
  }

  public static byte[] toBytes(final double d) {
    // Encode it as a long
    return Bytes.toBytes(Double.doubleToRawLongBits(d));
  }

  public static BigDecimal toBigDecimal(byte[] bytes) {
    return toBigDecimal(bytes, 0, bytes.length);
  }

  /**
   * Converts a byte array to a BigDecimal value
   *
   * @param bytes
   * @param offset
   * @param length
   * @return the char value
   */
  public static BigDecimal toBigDecimal(byte[] bytes, int offset, final int length) {
    if (bytes == null || length < SIZEOF_INT + 1 || (offset + length > bytes.length)) {
      return null;
    }

    int scale = toInt(bytes, offset);
    byte[] tcBytes = new byte[length - SIZEOF_INT];
    System.arraycopy(bytes, offset + SIZEOF_INT, tcBytes, 0, length - SIZEOF_INT);
    return new BigDecimal(new BigInteger(tcBytes), scale);
  }

  public static int toInt(byte[] bytes) {
    return toInt(bytes, 0, SIZEOF_INT);
  }

  public static int toInt(byte[] bytes, int offset) {
    return toInt(bytes, offset, SIZEOF_INT);
  }

  public static int toInt(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
    }
    int n = 0;
    for (int i = offset; i < (offset + length); i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }

  private static IllegalArgumentException explainWrongLengthOrOffset(final byte[] bytes,
      final int offset, final int length, final int expectedLength) {
    String reason;
    if (length != expectedLength) {
      reason = "Wrong length: " + length + ", expected " + expectedLength;
    } else {
      reason = "offset (" + offset + ") + length (" + length + ") exceed the"
          + " capacity of the array: " + bytes.length;
    }
    return new IllegalArgumentException(reason);
  }

}
