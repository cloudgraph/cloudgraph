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
package org.cloudgraph.hbase.service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.plasma.sdo.DataType;
import org.plasma.sdo.helper.DataConverter;

import com.google.common.primitives.UnsignedInteger;

import commonj.sdo.Property;

/**
 * HBase specific data converter, delegates to {@link DataConverter} under
 * certain conditions.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class HBaseDataConverter {
  private static Log log = LogFactory.getFactory().getInstance(HBaseDataConverter.class);

  static public volatile HBaseDataConverter INSTANCE = initializeInstance();

  private HBaseDataConverter() {
  }

  /**
   * Converts the given value to bytes based on data type and cardinality
   * information for the given property. For 'many' or multi-valued properties,
   * if the SDO Java type for the property is not already String, the value is
   * first converted to String using the SDO conversion which uses
   * java.util.Arrays formatting.
   * 
   * @param sourceProperty
   *          the source property
   * @param value
   *          the value
   * @return the bytes
   * @throws IllegalArgumentException
   *           if the given property is not a data type property
   */
  private static synchronized HBaseDataConverter initializeInstance() {
    if (INSTANCE == null)
      INSTANCE = new HBaseDataConverter();
    return INSTANCE;
  }

  /**
   * Converts the given value from bytes based on data type and cardinality
   * information for the given property. For 'many' or multi-valued properties,
   * if the SDO Java type for the property is not already String, the value is
   * first converted from a String using the SDO conversion which uses
   * java.util.Arrays formatting, resulting in an array of primitive types. For
   * non 'many' or singular properties, only the HBase Bytes utility is used.
   * 
   * @param targetProperty
   *          the property
   * @param value
   *          the bytes value
   * @return the converted object
   * @throws IllegalArgumentException
   *           if the given property is not a data type property
   */
  public Object fromBytes(Property targetProperty, byte[] value) {
    Object result = null;

    if (!targetProperty.getType().isDataType())
      throw new IllegalArgumentException("property " + targetProperty.toString()
          + " is not a datatype property");

    DataType targetDataType = DataType.valueOf(targetProperty.getType().getName());

    switch (targetDataType) {
    // Data types stored as String bytes in HBase
    case String:
    case Strings:
    case URI:
    case Month:
    case MonthDay:
    case Day:
    case Time:
    case Year:
    case YearMonth:
    case YearMonthDay:
    case Duration:
      String resultStr = Bytes.toString(value);
      result = DataConverter.INSTANCE.fromString(targetProperty, resultStr);
      break;
    case Date:
      resultStr = Bytes.toString(value);
      result = DataConverter.INSTANCE.fromString(targetProperty, resultStr);
      break;
    case DateTime:
      // NOTE: remember datetime is a String Java representation in
      // SDO 2.1
      resultStr = Bytes.toString(value);
      result = DataConverter.INSTANCE.fromString(targetProperty, resultStr);
      break;

    // Data types stored by directly converting from primitive types to
    // bytes in HBase.
    // FIXME: what to do with variable size types e.g. Decimal, BigInteger,
    // Bytes
    case Decimal:
      if (!targetProperty.isMany())
        result = Bytes.toBigDecimal(value);
      else
        result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
      break;
    case Bytes:
      if (!targetProperty.isMany())
        result = value; // already bytes
      else
        result = value; // already bytes
      break;
    case Byte:
      if (!targetProperty.isMany()) {
        // NOTE: no toByte method as would expect as there is
        // opposite method, see below
        // e.g. Bytes.toByte(value);
        if (value != null) {
          if (value.length > 2)
            log.warn("truncating " + String.valueOf(value.length)
                + " length byte array for target data type 'byte'");
          result = value[0];
        }
      } else {
        result = value;
      }
      break;
    case Boolean:
      if (!targetProperty.isMany()) {
        result = Bytes.toBoolean(value);
      } else {
        int count = value.length / Bytes.SIZEOF_BOOLEAN;
        boolean[] array = new boolean[count];
        for (int offset = 0, idx = 0; offset < value.length; offset += Bytes.SIZEOF_BOOLEAN, idx++) {
          byte[] buf = new byte[Bytes.SIZEOF_BOOLEAN];
          System.arraycopy(value, offset, buf, 0, Bytes.SIZEOF_BOOLEAN);
          array[idx] = Bytes.toBoolean(buf);
        }
        result = array;
      }
      break;
    case Character:
      if (!targetProperty.isMany())
        result = Character.valueOf(Bytes.toString(value).charAt(0));
      else
        result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
      break;
    case Double:
      if (!targetProperty.isMany()) {
        result = Bytes.toDouble(value);
      } else {
        int count = value.length / Bytes.SIZEOF_DOUBLE;
        double[] array = new double[count];
        for (int offset = 0, idx = 0; offset < value.length; offset += Bytes.SIZEOF_DOUBLE, idx++) {
          array[idx] = Bytes.toDouble(value, offset);
        }
        result = array;
      }
      break;
    case Float:
      if (!targetProperty.isMany()) {
        result = Bytes.toFloat(value);
      } else {
        int count = value.length / Bytes.SIZEOF_FLOAT;
        float[] array = new float[count];
        for (int offset = 0, idx = 0; offset < value.length; offset += Bytes.SIZEOF_FLOAT, idx++) {
          array[idx] = Bytes.toFloat(value, offset);
        }
        result = array;
      }
      break;
    case Int:
      if (!targetProperty.isMany()) {
        result = Bytes.toInt(value);
      } else {
        int count = value.length / Bytes.SIZEOF_INT;
        int[] array = new int[count];
        for (int offset = 0, idx = 0; offset < value.length; offset += Bytes.SIZEOF_INT, idx++) {
          array[idx] = Bytes.toInt(value, offset, Bytes.SIZEOF_INT);
        }
        result = array;
      }
      break;
    case Integer:
      if (!targetProperty.isMany())
        result = new BigInteger(value);
      else
        result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
      break;
    case Long:
      if (!targetProperty.isMany()) {
        result = Bytes.toLong(value);
      } else {
        int count = value.length / Bytes.SIZEOF_LONG;
        long[] array = new long[count];
        for (int offset = 0, idx = 0; offset < value.length; offset += Bytes.SIZEOF_LONG, idx++) {
          array[idx] = Bytes.toLong(value, offset, Bytes.SIZEOF_LONG);
        }
        result = array;
      }
      break;
    case Short:
      if (!targetProperty.isMany()) {
        result = Bytes.toShort(value);
      } else {
        int count = value.length / Bytes.SIZEOF_SHORT;
        short[] array = new short[count];
        for (int offset = 0, idx = 0; offset < value.length; offset += Bytes.SIZEOF_SHORT, idx++) {
          array[idx] = Bytes.toShort(value, offset, Bytes.SIZEOF_SHORT);
        }
        result = array;
      }
      break;
    case Object:
      // FIXME: custom serialization?
    default:
      result = Bytes.toString(value);
      break;
    }

    return result;
  }

  /**
   * Converts the given value to bytes based on data type and cardinality
   * information for the given property. For 'many' or multi-valued properties,
   * if the SDO Java type for the property is not already String, the value is
   * first converted to String using the SDO conversion which uses
   * java.util.Arrays formatting. For non 'many' or singular properties, only
   * the HBase Bytes utility is used.
   * 
   * @param sourceProperty
   *          the source property
   * @param value
   *          the value
   * @return the bytes
   * @throws IllegalArgumentException
   *           if the given property is not a data type property
   */
  public byte[] toBytes(Property sourceProperty, Object value) {
    byte[] result;

    if (!sourceProperty.getType().isDataType())
      throw new IllegalArgumentException("property " + sourceProperty.toString()
          + " is not a datatype property");
    DataType dataType = DataType.valueOf(sourceProperty.getType().getName());

    switch (dataType) {
    // Data types stored as String bytes in HBase
    case String:
    case Strings:
    case URI:
    case Month:
    case MonthDay:
    case Day:
    case Time:
    case Year:
    case YearMonth:
    case YearMonthDay:
    case Date:
    case Duration:
      String resultStr = DataConverter.INSTANCE.toString(sourceProperty, value);
      result = Bytes.toBytes(resultStr);
      break;
    case DateTime:
      resultStr = DataConverter.INSTANCE.toString(sourceProperty, value);
      result = Bytes.toBytes(resultStr);
      break;
    // Data types stored by directly converting from primitive types to
    // bytes in HBase.
    // FIXME: When the given property is a 'many' property and has a variable
    // length, the value is first
    // converted to String so
    // can be delimited.
    case Decimal:
      if (!sourceProperty.isMany()) {
        BigDecimal resultDecimal = DataConverter.INSTANCE
            .toDecimal(sourceProperty.getType(), value);
        result = Bytes.toBytes(resultDecimal);
      } else {
        String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
        result = Bytes.toBytes(strResult);
      }
      break;
    case Bytes:
      if (!sourceProperty.isMany()) {
        byte[] resultBytes = DataConverter.INSTANCE.toBytes(sourceProperty.getType(), value);
        result = resultBytes;
      } else {
        String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
        result = Bytes.toBytes(strResult);
      }
      break;
    case Byte:
      if (!sourceProperty.isMany()) {
        byte resultByte = DataConverter.INSTANCE.toByte(sourceProperty.getType(), value);
        result = Bytes.toBytes(resultByte);
      } else {         
        result = (byte[]) value;
      }
      break;
    case Boolean:
      if (!sourceProperty.isMany()) {
        boolean resultBool = DataConverter.INSTANCE.toBoolean(sourceProperty.getType(), value);
        result = Bytes.toBytes(resultBool);
      } else {        
        boolean[] list = (boolean[]) value;
        result = new byte[list.length * Bytes.SIZEOF_BOOLEAN];
        int pos = 0;
        for (boolean val : list) {
          byte[] bytesVal = Bytes.toBytes(val);
          System.arraycopy(bytesVal, 0, result, pos, Bytes.SIZEOF_BOOLEAN);
          pos += Bytes.SIZEOF_BOOLEAN;
        }
      }
      break;
    case Character:
      if (!sourceProperty.isMany()) {
        resultStr = DataConverter.INSTANCE.toString(sourceProperty.getType(), value);
        result = Bytes.toBytes(resultStr);
      } else {
        String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
        result = Bytes.toBytes(strResult);
      }
      break;
    case Double:
      if (!sourceProperty.isMany()) {
        double resultDouble = DataConverter.INSTANCE.toDouble(sourceProperty.getType(), value);
        result = Bytes.toBytes(resultDouble);
      } else {         
        double[] array = (double[]) value;
        result = new byte[array.length * Bytes.SIZEOF_LONG];
        int pos = 0;
        for (double val : array) {
          byte[] bytesVal = Bytes.toBytes(val);
          System.arraycopy(bytesVal, 0, result, pos, Bytes.SIZEOF_LONG);
          pos += Bytes.SIZEOF_LONG;
        }
      }
      break;
    case Float:
      if (!sourceProperty.isMany()) {
        float resultFloat = DataConverter.INSTANCE.toFloat(sourceProperty.getType(), value);
        result = Bytes.toBytes(resultFloat);
      } else {
        float[] array = (float[]) value;
        result = new byte[array.length * Bytes.SIZEOF_INT];
        int pos = 0;
        for (float val : array) {
          byte[] bytesVal = Bytes.toBytes(val);
          System.arraycopy(bytesVal, 0, result, pos, Bytes.SIZEOF_INT);
          pos += Bytes.SIZEOF_INT;
        }
      }
      break;
    case Int:
      if (!sourceProperty.isMany()) {
        int resultInt = DataConverter.INSTANCE.toInt(sourceProperty.getType(), value);
        result = Bytes.toBytes(resultInt);
      } else {
        int[] array = (int[]) value;
        result = new byte[array.length * Bytes.SIZEOF_INT];
        int pos = 0;
        for (int val : array) {
          byte[] bytesVal = Bytes.toBytes(val);
          System.arraycopy(bytesVal, 0, result, pos, Bytes.SIZEOF_INT);
          pos += Bytes.SIZEOF_INT;
        }
      }
      break;
    case Integer:
      if (!sourceProperty.isMany()) {
        BigInteger resultInteger = DataConverter.INSTANCE
            .toInteger(sourceProperty.getType(), value);
        result = resultInteger.toByteArray();
      } else {
        String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
        result = Bytes.toBytes(strResult);
      }
      break;
    case Long:
      if (!sourceProperty.isMany()) {
        long resultLong = DataConverter.INSTANCE.toLong(sourceProperty.getType(), value);
        result = Bytes.toBytes(resultLong);
      } else {
        long[] array = (long[]) value;
        result = new byte[array.length * Bytes.SIZEOF_LONG];
        int pos = 0;
        for (long val : array) {
          byte[] bytesVal = Bytes.toBytes(val);
          System.arraycopy(bytesVal, 0, result, pos, Bytes.SIZEOF_LONG);
          pos += Bytes.SIZEOF_LONG;
        }
      }
      break;
    case Short:
      if (!sourceProperty.isMany()) {
        short resultShort = DataConverter.INSTANCE.toShort(sourceProperty.getType(), value);
        result = Bytes.toBytes(resultShort);
      } else {
        short[] array = (short[]) value;
        result = new byte[array.length * Bytes.SIZEOF_SHORT];
        int pos = 0;
        for (short val : array) {
          byte[] bytesVal = Bytes.toBytes(val);
          System.arraycopy(bytesVal, 0, result, pos, Bytes.SIZEOF_SHORT);
          pos += Bytes.SIZEOF_SHORT;
        }
      }
      break;
    case Object:
      // FIXME: do we serialize objects in some custom format for
      // hbase
    default:
      resultStr = DataConverter.INSTANCE.toString(sourceProperty.getType(), value);
      result = Bytes.toBytes(resultStr);
      break;
    }
    return result;
  }

}
