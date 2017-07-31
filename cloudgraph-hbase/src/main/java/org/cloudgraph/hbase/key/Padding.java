/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.hbase.key;

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
