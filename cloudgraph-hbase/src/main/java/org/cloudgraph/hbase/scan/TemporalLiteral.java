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
package org.cloudgraph.hbase.scan;

import java.util.Arrays;
import java.util.Date;

import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Type;

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
 * @see org.cloudgraph.store.mapping.TableMapping
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
      RelationalOperatorName relationalOperator, DataRowKeyFieldMapping fieldConfig,
      StoreMappingContext mappingContext) {
    super(literal, rootType, relationalOperator, fieldConfig, mappingContext);
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
    return this.literalToBytes();
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
    return this.nextLiteralToBytes();
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
    return this.nextLiteralToBytes();
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
    // Note: the partial scan stop row bytes are exclusive
    // which is why we are incrementing below.
    return this.literalToBytes();
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
    if (this.fieldConfig.getCodec().isLexicographic()
        && !this.fieldConfig.getCodec().isTransforming())
      return getEqualsStartBytes();
    else
      throw new ScanException("cannot create fuzzy scan literal " + "for "
          + this.fieldConfig.getCodecType() + " encoded key field with path '"
          + this.fieldConfig.getPropertyPath() + "' within table "
          + this.table.getQualifiedPhysicalName() + " for graph root type, "
          + this.rootType.toString());
  }

  @Override
  public byte[] getFuzzyInfoBytes() {
    byte[] infoBytes = new byte[this.fieldConfig.getMaxLength()];
    Arrays.fill(infoBytes, (byte) 0); // fixed char
    return infoBytes;
  }

  private byte[] literalToBytes() {
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    return this.fieldConfig.getCodec().encode(value);
  }

  private byte[] nextLiteralToBytes() {
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    return this.fieldConfig.getCodec().encodeNext(value);
  }

  /**
   * As per SDO 2.1 spec every temporal data type can be converted to a date,
   * not a long however. So get to a date then a long, then manipulate/increment
   * the value and convert back..
   * 
   * @param type
   * @param value
   * @return
   */
  private String increment(Type type, Object value) {
    Date dateValue = this.dataConverter.toDate(property.getType(), value);
    Long longValue = dateValue.getTime();
    DataType dataType = DataType.valueOf(property.getType().getName());
    Long incrementLongValue = longValue + getIncrement(dataType);
    Date incrementDate = new Date(incrementLongValue);
    // back to whatever its native type is
    Object incrementValue = this.dataConverter.convert(property.getType(), incrementDate);
    // re format the string under its native type
    String result = this.dataConverter.toString(property.getType(), incrementValue);
    return result;
  }

}
