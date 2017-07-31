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

import java.util.Arrays;
import java.util.Date;

import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.cloudgraph.hbase.service.HBaseDataConverter;
import org.plasma.query.model.RelationalOperator;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaType;

/**
 * A temporal data "flavor" specific literal class used to abstract the
 * complexities involved in assembling the various segments and fields of
 * composite (scan start/stop) row keys under various relational and logical
 * operator and various configurable composite key-field hashing, formatting,
 * padding and other features. A temporal literal does not contain or involve
 * wildcards but nevertheless may "participate" in a fuzzy scan as part of a
 * composite row key and therefore implements {@link FuzzyRowKeyLiteral}
 * supplying only default key and info bytes.
 * 
 * @see org.cloudgraph.config.TableConfig
 * @see org.cloudgraph.hbase.service.HBaseDataConverter
 */
public class TemporalLiteral extends ScanLiteral implements PartialRowKeyLiteral,
    FuzzyRowKeyLiteral, CompleteRowKeyLiteral {

  public static final int INCREMENT = 1;
  public static final int DATE_INCREMENT = 1000; // SDO Date data type
  // resolution is seconds
  public static final int DATE_TIME_INCREMENT = 1; // SDO Datetime data type

  // resolution is seconds

  public TemporalLiteral(String literal, PlasmaType rootType,
      RelationalOperator relationalOperator, UserDefinedRowKeyFieldConfig fieldConfig) {
    super(literal, rootType, relationalOperator, fieldConfig);
  }

  protected int getIncrement(DataType dataType) {
    switch (dataType) {
    case Date:
      return DATE_INCREMENT;
    case DateTime:
      return DATE_TIME_INCREMENT;
    default:
      return INCREMENT;
    }
  }

  /**
   * Returns the "start row" bytes used to represent "equals" relational
   * operator under an HBase partial row-key scan for this string (data flavor)
   * literal under the various optionally configurable hashing, formatting and
   * padding features.
   * 
   * @return the "start row" bytes used to represent "equals" relational
   *         operator under an HBase partial row-key scan for this string (data
   *         flavor) literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getEqualsStartBytes() {
    byte[] startBytes = null;
    // FIXME: convert to native type as we create literals
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    if (fieldConfig.isHash()) {
      startBytes = HBaseDataConverter.INSTANCE.toBytes(property, value);
      startBytes = this.hashing.toStringBytes(startBytes);
      startBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.temporal);
    } else {
      startBytes = HBaseDataConverter.INSTANCE.toBytes(property, value);
      startBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.temporal);
    }
    return startBytes;
  }

  /**
   * Returns the "stop row" bytes used to represent "equals" relational operator
   * under an HBase partial row-key scan for this string (data flavor) literal
   * under the various optionally configurable hashing, formatting and padding
   * features.
   * 
   * @return the "stop row" bytes used to represent "equals" relational operator
   *         under an HBase partial row-key scan for this string (data flavor)
   *         literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getEqualsStopBytes() {
    byte[] stopBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    DataType dataType = DataType.valueOf(property.getType().getName());
    // As per SDO 2.1 spec every temporal data type can be
    // converted to a date, not a long however.
    // So get to a date then a long, then manipulate/increment the
    // value and convert back...
    Date dateValue = this.dataConverter.toDate(property.getType(), value);
    Long longValue = dateValue.getTime();

    // Note: the partial scan stop row bytes are exclusive
    // which is why we are incrementing below.

    if (this.fieldConfig.isHash()) {
      stopBytes = HBaseDataConverter.INSTANCE.toBytes(property, value);
      stopBytes = this.hashing.toStringBytes(stopBytes, HASH_INCREMENT);
      stopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(), DataFlavor.temporal);
    } else {
      Long stopLongValue = longValue + getIncrement(dataType);
      Date stopDate = new Date(stopLongValue);
      // back to whatever its native type is
      Object stopValue = this.dataConverter.convert(property.getType(), stopDate);
      // re format the string under its native type
      String stopValueStr = this.dataConverter.toString(property.getType(), stopValue);
      stopBytes = stopValueStr.getBytes(this.charset);
      stopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(), DataFlavor.temporal);
    }
    return stopBytes;
  }

  /**
   * Returns the "start row" bytes used to represent "greater than" relational
   * operator under an HBase partial row-key scan for this string (data flavor)
   * literal under the various optionally configurable hashing, formatting and
   * padding features.
   * 
   * @return the "start row" bytes used to represent "greater than" relational
   *         operator under an HBase partial row-key scan for this string (data
   *         flavor) literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getGreaterThanStartBytes() {
    byte[] startBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    DataType dataType = DataType.valueOf(property.getType().getName());
    // As per SDO 2.1 spec every temporal data type can be
    // converted to a date, not a long however.
    // So get to a date then a long, then manipulate/increment the
    // value and convert back...
    Date dateValue = this.dataConverter.toDate(property.getType(), value);
    Long longValue = dateValue.getTime();

    if (this.fieldConfig.isHash()) {
      startBytes = HBaseDataConverter.INSTANCE.toBytes(property, value);
      startBytes = this.hashing.toStringBytes(startBytes, HASH_INCREMENT);
      startBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.temporal);
    } else {
      Long startLongValue = longValue + getIncrement(dataType);
      Date startDate = new Date(startLongValue);
      // back to whatever its native type is
      Object startValue = this.dataConverter.convert(property.getType(), startDate);
      // re format the string under its native type
      String startValueStr = this.dataConverter.toString(property.getType(), startValue);

      startBytes = startValueStr.getBytes(this.charset);
      startBytes = this.padding.pad(startBytes, this.fieldConfig.getMaxLength(),
          DataFlavor.temporal);
    }
    return startBytes;
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
   * relational operator under an HBase partial row-key scan for this string
   * (data flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "greater than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         string (data flavor) literal under the various optionally
   *         configurable hashing, formatting and padding features.
   */
  public byte[] getGreaterThanEqualStartBytes() {
    return this.getEqualsStartBytes();
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
   * operator under an HBase partial row-key scan for this string (data flavor)
   * literal under the various optionally configurable hashing, formatting and
   * padding features.
   * 
   * @return the "stop row" bytes used to represent "less than" relational
   *         operator under an HBase partial row-key scan for this string (data
   *         flavor) literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getLessThanStopBytes() {
    byte[] stopBytes = null;
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    // As per SDO 2.1 spec every temporal data type can be
    // converted to a date, not a long however.
    // So get to a date then a long, then manipulate/increment the
    // value and convert back...
    Date dateValue = this.dataConverter.toDate(property.getType(), value);
    Long longValue = dateValue.getTime();

    // Note: the partial scan stop row bytes are exclusive
    // which is why we are incrementing below.

    if (this.fieldConfig.isHash()) {
      stopBytes = HBaseDataConverter.INSTANCE.toBytes(property, value);
      stopBytes = this.hashing.toStringBytes(stopBytes);
      stopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(), DataFlavor.temporal);
    } else {
      Long stopLongValue = longValue;
      Date stopDate = new Date(stopLongValue);
      // back to whatever its native type is
      Object stopValue = this.dataConverter.convert(property.getType(), stopDate);
      // re format the string under its native type
      String stopValueStr = this.dataConverter.toString(property.getType(), stopValue);

      stopBytes = stopValueStr.getBytes(this.charset);
      stopBytes = this.padding.pad(stopBytes, this.fieldConfig.getMaxLength(), DataFlavor.temporal);
    }
    return stopBytes;
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
   * relational operator under an HBase partial row-key scan for this string
   * (data flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "less than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         string (data flavor) literal under the various optionally
   *         configurable hashing, formatting and padding features.
   */
  public byte[] getLessThanEqualStopBytes() {
    return this.getEqualsStopBytes();
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

  @Override
  public byte[] getFuzzyKeyBytes() {
    return getEqualsStartBytes();
  }

  @Override
  public byte[] getFuzzyInfoBytes() {
    byte[] infoBytes = new byte[this.fieldConfig.getMaxLength()];
    Arrays.fill(infoBytes, (byte) 0); // fixed char
    return infoBytes;
  }
}
