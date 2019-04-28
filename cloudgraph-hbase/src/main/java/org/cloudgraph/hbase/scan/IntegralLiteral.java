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

import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.sdo.PlasmaType;

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
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.hbase.service.HBaseDataConverter
 * @author Scott Cinnamond
 * @since 0.5
 */
public class IntegralLiteral extends ScanLiteral implements PartialRowKeyLiteral,
    FuzzyRowKeyLiteral, CompleteRowKeyLiteral {

  public static final int INCREMENT = 1;

  public IntegralLiteral(String literal, PlasmaType rootType,
      RelationalOperatorName relationalOperator, DataRowKeyFieldMapping fieldConfig,
      StoreMappingContext mappingContext) {
    super(literal, rootType, relationalOperator, fieldConfig, mappingContext);
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
    return literalToBytes();
    //
    // if (this.fieldConfig.isHash()) {
    // startBytes = this.hashing.toStringBytes(startValueStr);
    // paddedStartBytes = this.padding.pad(startBytes,
    // this.fieldConfig.getMaxLength(),
    // DataFlavor.integral);
    // } else {
    // startBytes = startValueStr.getBytes(this.charset);
    // paddedStartBytes = this.padding.pad(startBytes,
    // this.fieldConfig.getMaxLength(),
    // this.fieldConfig.getDataFlavor());
    // }
    // return paddedStartBytes;
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
    return this.nextLiteralToBytes();
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
    return this.literalToBytes();
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
    // Note: in HBase the stop row is exclusive, so just use
    // the literal value, no need to decrement it
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
    // Note: in HBase the stop row is exclusive, so increment
    // stop value to get this row for this field/literal
    return this.nextLiteralToBytes();
  }

  @Override
  public byte[] getFuzzyKeyBytes() {
    if (this.fieldConfig.getCodec().isLexicographic()
        && !this.fieldConfig.getCodec().isTransforming()) {
      Object value = this.dataConverter.convert(property.getType(), this.literal);
      return this.fieldConfig.getCodec().encode(value);
    } else
      throw new ScanException("cannot create fuzzy scan literal " + "for "
          + this.fieldConfig.getCodecType() + " encoded key field with path '"
          + this.fieldConfig.getPropertyPath() + "' within table "
          + this.table.getQualifiedPhysicalName() + " for graph root type, "
          + this.rootType.toString());
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

  private byte[] literalToBytes() {
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    return this.fieldConfig.getCodec().encode(value);
  }

  private byte[] nextLiteralToBytes() {
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    return this.fieldConfig.getCodec().encodeNext(value);
  }
}
