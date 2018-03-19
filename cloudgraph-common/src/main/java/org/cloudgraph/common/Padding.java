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

import java.nio.charset.Charset;
import java.util.Arrays;

import org.plasma.sdo.DataFlavor;

/**
 * Delegate for composite row and column key padding.
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class Padding {
  private Charset charset;
  private byte zero;
  private byte space;

  @SuppressWarnings("unused")
  private Padding() {
  }

  public Padding(Charset charset) {
    this.charset = charset;
    this.zero = (byte) Character.valueOf('0').charValue();
    this.space = (byte) Character.valueOf(' ').charValue();
  }

  /**
   * Returns an array front padded with zeros or rear padded with spaces
   * depending on the given data flavor. Returns the given array as is if the
   * given array size is less than or equal to the given max length.
   * 
   * @param value
   *          the value
   * @param maxLength
   *          the maximum length of the target key field
   * @param dataFlavor
   *          the data flavor
   * @return an array front padded with zeros or rear padded with spaces
   *         depending on the given data flavor. Returns the given array as is
   *         if the given array size is less than or equal to the given max
   *         length.
   */
  public final byte[] pad(byte[] value, int maxLength, DataFlavor dataFlavor) {

    int len = value.length;
    int delta = maxLength - len;
    if (delta <= 0)
      return value;
    byte[] result = null;

    switch (dataFlavor) {
    case integral:
    case real: // front pad with zeros
      result = front(value, maxLength, zero);
      break;
    case temporal:
    case string:
    case other:
    default: // end pad with spaces
      result = back(value, maxLength, space);
      break;
    }
    return result;
  }

  public final byte[] strip(byte[] value, int maxLength, DataFlavor dataFlavor) {
    byte[] result = null;
    switch (dataFlavor) {
    case integral:
    case real: // front pad with zeros
      result = strip(value, maxLength, zero);
      break;
    case temporal:
    case string:
    case other:
    default: // end pad with spaces
      result = strip(value, maxLength, space);
      break;
    }
    return result;
  }

  public byte[] front(byte[] src, int maxLength, byte pad) {
    byte[] result = new byte[maxLength];
    int delta = maxLength - src.length;
    Arrays.fill(result, 0, delta, pad);
    System.arraycopy(src, 0, result, delta, src.length);
    return result;
  }

  public byte[] back(byte[] src, int maxLength, byte pad) {
    byte[] result = new byte[maxLength];
    int delta = maxLength - src.length;
    System.arraycopy(src, 0, result, 0, src.length);
    Arrays.fill(result, src.length, src.length + delta, pad);
    return result;
  }

  public byte[] strip(byte[] src, int maxLength, byte pad) {
    if (src.length != maxLength)
      throw new IllegalArgumentException("expected src length of " + maxLength);
    byte[] stripped = new byte[maxLength];
    int idx = 0;
    for (int i = 0; i < maxLength; i++) {
      if (src[i] != pad) {
        stripped[idx] = src[i];
        idx++;
      }
    }
    byte[] result = new byte[idx];
    System.arraycopy(stripped, 0, result, 0, result.length);
    return result;
  }

  /**
   * Returns an array front padded with zeros or rear padded with spaces
   * depending on the given data flavor.
   * 
   * @param value
   *          the value
   * @param maxLength
   *          the maximum length of the target key field
   * @param dataFlavor
   *          the data flavor
   * @return an array front padded with zeros or rear padded with spaces
   *         depending on the given data flavor.
   */
  public final String pad(String value, int maxLength, DataFlavor dataFlavor) {
    int len = value.length();
    int delta = maxLength - len;
    if (delta < 0)
      return value;
    String result = null;

    switch (dataFlavor) {
    case integral:
    case real: // fount pad with zeros
      result = front(value, maxLength, '0');
      break;
    case temporal:
    case string:
    case other:
    default: // end pad with spaces
      result = back(value, maxLength, ' ');
      break;
    }

    return new String(result);
  }

  public String front(String src, int maxLength, char pad) {
    char[] result = new char[maxLength];
    char[] srcchars = src.toCharArray();
    int delta = maxLength - srcchars.length;
    int offset = delta;
    Arrays.fill(result, 0, delta, pad);
    System.arraycopy(srcchars, 0, result, offset, srcchars.length);
    return new String(result);
  }

  public String back(String src, int maxLength, char pad) {
    char[] result = new char[maxLength];
    char[] srcchars = src.toCharArray();
    int delta = maxLength - srcchars.length;
    System.arraycopy(srcchars, 0, result, 0, srcchars.length);
    Arrays.fill(result, srcchars.length, srcchars.length + delta, pad);
    return new String(result);
  }

}
