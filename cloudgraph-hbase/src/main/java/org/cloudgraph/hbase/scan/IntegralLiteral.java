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
package org.cloudgraph.hbase.scan;

import java.math.BigInteger;
import java.util.Arrays;

import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.plasma.query.model.RelationalOperator;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Type;

/**
 * An integral data "flavor" specific literal class used to abstract the
 * complexities involved in assembling the various segments and fields of
 * composite (scan start/stop) row keys under various relational and logical
 * operator and various configurable composite key-field hashing, formatting,
 * padding and other features. An integral literal does not contain or involve
 * wildcards but nevertheless may "participate" in a fuzzy scan as part of a
 * composite row key and therefore implements {@link FuzzyRowKeyLiteral}
 * supplying only default key and info bytes.
 * 
 * @see org.cloudgraph.config.TableConfig
 * @see org.cloudgraph.hbase.service.HBaseDataConverter
 * @author Scott Cinnamond
 * @since 0.5
 */
public class IntegralLiteral extends ScanLiteral implements PartialRowKeyLiteral,
    FuzzyRowKeyLiteral, CompleteRowKeyLiteral {

  public static final int INCREMENT = 1;

  public IntegralLiteral(String literal, PlasmaType rootType,
      RelationalOperator relationalOperator, UserDefinedRowKeyFieldConfig fieldConfig) {
    super(literal, rootType, relationalOperator, fieldConfig);
  }

  /**
   * Returns the "start row" bytes used to represent "equals" relational
   * operator under an HBase partial row-key scan for this integral (data
   * flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "equals" relational
   *         operator under an HBase partial row-key scan for this integral
   *         (data flavor) literal under the various optionally configurable
   *         hashing, formatting and padding features.
   */
  public byte[] getEqualsStartBytes() {
    byte[] startBytes = null;
    byte[] paddedStartBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    String startValueStr = this.dataConverter.toString(property.getType(), value);
    if (this.fieldConfig.isHash()) {
      startBytes = this.hashing.toStringBytes(startValueStr);
      paddedStartBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.integral);
    } else {
      startBytes = startValueStr.getBytes(this.charset);
      paddedStartBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          this.fieldConfig.getDataFlavor());
    }
    return paddedStartBytes;
  }

  /**
   * Returns the "stop row" bytes used to represent "equals" relational operator
   * under an HBase partial row-key scan for this integral (data flavor) literal
   * under the various optionally configurable hashing, formatting and padding
   * features.
   * 
   * @return the "stop row" bytes used to represent "equals" relational operator
   *         under an HBase partial row-key scan for this integral (data flavor)
   *         literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getEqualsStopBytes() {
    byte[] stopBytes = null;
    byte[] paddedStopBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);

    if (this.fieldConfig.isHash()) {
      String stopValueStr = this.dataConverter.toString(property.getType(), value);
      stopBytes = this.hashing.toStringBytes(stopValueStr, HASH_INCREMENT);
      paddedStopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.integral);
    } else {
      String stopValueStr = increment(property.getType(), value);
      stopBytes = stopValueStr.getBytes(this.charset);
      paddedStopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(),
          this.fieldConfig.getDataFlavor());
    }
    return paddedStopBytes;
  }

  /**
   * Returns the "start row" bytes used to represent "greater than" relational
   * operator under an HBase partial row-key scan for this integral (data
   * flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "greater than" relational
   *         operator under an HBase partial row-key scan for this integral
   *         (data flavor) literal under the various optionally configurable
   *         hashing, formatting and padding features.
   */
  public byte[] getGreaterThanStartBytes() {
    byte[] startBytes = null;
    byte[] paddedStartBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    if (fieldConfig.isHash()) {
      String startValueStr = this.dataConverter.toString(property.getType(), value);
      startBytes = this.hashing.toStringBytes(startValueStr, HASH_INCREMENT);
      paddedStartBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.integral);
    } else {
      String startValueStr = increment(property.getType(), value);
      startBytes = startValueStr.getBytes(this.charset);
      paddedStartBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          this.fieldConfig.getDataFlavor());
    }
    return paddedStartBytes;
  }

  /**
   * The "greater than" relational operator does not effect the stop bytes for
   * an HBase partial row-key scan and this method therefore returns an empty
   * byte array or "no-op".
   * 
   * @return an empty byte array or "no-op".
   */
  public byte[] getGreaterThanStopBytes() {
    return new byte[0];
  }

  /**
   * Returns the "start row" bytes used to represent "greater than equals"
   * relational operator under an HBase partial row-key scan for this integral
   * (data flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "greater than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         integral (data flavor) literal under the various optionally
   *         configurable hashing, formatting and padding features.
   */
  public byte[] getGreaterThanEqualStartBytes() {
    byte[] startBytes = null;
    byte[] paddedStartBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    String startValueStr = this.dataConverter.toString(property.getType(), value);
    if (fieldConfig.isHash()) {
      startBytes = this.hashing.toStringBytes(startValueStr);
      paddedStartBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.integral);
    } else {
      startBytes = startValueStr.getBytes(this.charset);
      paddedStartBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          this.fieldConfig.getDataFlavor());
    }
    return paddedStartBytes;
  }

  /**
   * The "greater than equals" relational operator does not effect the stop
   * bytes for an HBase partial row-key scan and this method therefore returns
   * an empty byte array or "no-op".
   * 
   * @return an empty byte array or "no-op".
   */
  public byte[] getGreaterThanEqualStopBytes() {
    return new byte[0];
  }

  /**
   * The "less than" relational operator does not effect the start bytes for an
   * HBase partial row-key scan and this method therefore returns an empty byte
   * array or "no-op".
   * 
   * @return an empty byte array or "no-op".
   */
  public byte[] getLessThanStartBytes() {
    return new byte[0];
  }

  /**
   * Returns the "stop row" bytes used to represent "less than" relational
   * operator under an HBase partial row-key scan for this integral (data
   * flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "less than" relational
   *         operator under an HBase partial row-key scan for this integral
   *         (data flavor) literal under the various optionally configurable
   *         hashing, formatting and padding features.
   */
  public byte[] getLessThanStopBytes() {
    byte[] stopBytes = null;
    byte[] paddedStopBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    // Note: in HBase the stop row is exclusive, so just use
    // the literal value, no need to decrement it
    String stopValueStr = this.dataConverter.toString(property.getType(), value);
    if (fieldConfig.isHash()) {
      stopBytes = this.hashing.toStringBytes(stopValueStr);
      paddedStopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.integral);
    } else {
      stopBytes = stopValueStr.getBytes(this.charset);
      paddedStopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(),
          this.fieldConfig.getDataFlavor());
    }
    return paddedStopBytes;
  }

  /**
   * The "less than equal" relational operator does not effect the start bytes
   * for an HBase partial row-key scan and this method therefore returns an
   * empty byte array or "no-op".
   * 
   * @return an empty byte array or "no-op".
   */
  public byte[] getLessThanEqualStartBytes() {
    return new byte[0];
  }

  /**
   * Returns the "stop row" bytes used to represent "less than equals"
   * relational operator under an HBase partial row-key scan for this integral
   * (data flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "less than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         integral (data flavor) literal under the various optionally
   *         configurable hashing, formatting and padding features.
   */
  public byte[] getLessThanEqualStopBytes() {
    byte[] stopBytes = null;
    byte[] paddedStopBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    // Note: in HBase the stop row is exclusive, so increment
    // stop value to get this row for this field/literal
    if (fieldConfig.isHash()) {
      String stopValueStr = this.dataConverter.toString(property.getType(), value);
      stopBytes = this.hashing.toStringBytes(stopValueStr, this.HASH_INCREMENT);
      paddedStopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.integral);
    } else {
      String stopValueStr = increment(property.getType(), value);
      stopBytes = stopValueStr.getBytes(this.charset);
      paddedStopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(),
          this.fieldConfig.getDataFlavor());
    }
    return paddedStopBytes;
  }

  @Override
  public byte[] getFuzzyKeyBytes() {
    byte[] keyBytes = null;
    byte[] paddedKeyBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    String valueStr = this.dataConverter.toString(property.getType(), value);
    if (fieldConfig.isHash()) {
      keyBytes = this.hashing.toStringBytes(valueStr);
      paddedKeyBytes = this.padding.pad(keyBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.integral);
    } else {
      keyBytes = valueStr.getBytes(this.charset);
      paddedKeyBytes = this.padding.pad(keyBytes, this.fieldConfig.getMaxLength(),
          this.fieldConfig.getDataFlavor());
    }

    return paddedKeyBytes;
  }

  @Override
  public byte[] getFuzzyInfoBytes() {
    byte[] infoBytes = new byte[this.fieldConfig.getMaxLength()];
    Arrays.fill(infoBytes, (byte) 0); // fuzzy filter fixed char
    return infoBytes;
  }

  /**
   * Returns the bytes used to represent an "equals" relational operator for a
   * specific composite row key field, under an HBase 'Get' operation for the
   * various optionally configurable hashing, formatting and padding features.
   * 
   * @return the bytes used to represent an "equals" relational operator for a
   *         specific composite row key field, under an HBase 'Get' operation
   *         for the various optionally configurable hashing, formatting and
   *         padding features.
   */
  @Override
  public byte[] getEqualsBytes() {
    return getEqualsStartBytes();
  }

  private String increment(Type type, Object value) {
    String result = "";
    DataType sourceDataType = DataType.valueOf(type.getName());
    switch (sourceDataType) {
    case Short:
      Short shortValue = this.dataConverter.toShort(property.getType(), value);
      Short shortResult = Short.valueOf((short) (shortValue.shortValue() + INCREMENT));
      result = this.dataConverter.toString(type, shortResult);
      break;
    case Int:
      Integer intValue = this.dataConverter.toInt(property.getType(), value);
      Integer intResult = Integer.valueOf(intValue.intValue() + INCREMENT);
      result = this.dataConverter.toString(type, intResult);
      break;
    case Long:
      Long longValue = this.dataConverter.toLong(property.getType(), value);
      Long longResult = Long.valueOf(longValue.longValue() + INCREMENT);
      result = this.dataConverter.toString(type, longResult);
      break;
    case Integer:
      BigInteger integerValue = this.dataConverter.toInteger(property.getType(), value);
      BigInteger integerResult = integerValue.add(BigInteger.valueOf(INCREMENT));
      result = this.dataConverter.toString(type, integerResult);
      break;
    default:
      throw new ScanException("expected integral (Float, Double, Decinal)datatype not, "
          + sourceDataType.name());
    }
    return result;
  }
}
