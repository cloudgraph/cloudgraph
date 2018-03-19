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
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.sdo.PlasmaType;

/**
 * A string data "flavor" specific literal class used to abstract the
 * complexities involved in assembling the various segments and fields of
 * composite (scan start/stop) row keys under various relational and logical
 * operator and various configurable composite key-field hashing, formatting,
 * padding and other features. A string literal does not contain or involve
 * wildcards, see {@link WildcardStringLiteral}, but nevertheless may
 * "participate" in a fuzzy scan as part of a composite row key and therefore
 * implements {@link FuzzyRowKeyLiteral} supplying only default key and info
 * bytes.
 * 
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.hbase.service.HBaseDataConverter
 * @see WildcardStringLiteral
 * @author Scott Cinnamond
 * @since 0.5
 */
public class StringLiteral extends ScanLiteral implements PartialRowKeyLiteral, FuzzyRowKeyLiteral,
    CompleteRowKeyLiteral {

  public static final byte INCREMENT = Byte.MIN_VALUE;

  public StringLiteral(String literal, PlasmaType rootType,
      RelationalOperatorName relationalOperator, DataRowKeyFieldMapping fieldConfig) {
    super(literal, rootType, relationalOperator, fieldConfig);
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
  @Override
  public byte[] getEqualsStartBytes() {
    return this.fieldConfig.getCodec().encode(this.literal);
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
  @Override
  public byte[] getEqualsStopBytes() {
    return this.fieldConfig.getCodec().encodeNext(this.literal);
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
  @Override
  public byte[] getGreaterThanStartBytes() {
    return this.fieldConfig.getCodec().encodeNext(this.literal);
  }

  /**
   * The "greater than" relational operator does not effect the stop bytes for
   * an HBase partial row-key scan and this method therefore returns an empty
   * byte array or "no-op".
   * 
   * @return an empty byte array or "no-op".
   */
  @Override
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
  @Override
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
  @Override
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
  @Override
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
  @Override
  public byte[] getLessThanStopBytes() {
    // Note: in HBase the stop row is exclusive, so just use
    // the literal value, no need to decrement it
    return this.fieldConfig.getCodec().encode(this.literal);
  }

  /**
   * The "less than equal" relational operator does not effect the start bytes
   * for an HBase partial row-key scan and this method therefore returns an
   * empty byte array or "no-op".
   * 
   * @return an empty byte array or "no-op".
   */
  @Override
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
  @Override
  public byte[] getLessThanEqualStopBytes() {

    // Note: in HBase the stop row is exclusive, so increment
    // stop value to get this row for this field/literal
    return this.fieldConfig.getCodec().encodeNext(this.literal);
  }

  @Override
  public byte[] getFuzzyKeyBytes() {
    if (this.fieldConfig.getCodec().isLexicographic()
        && !this.fieldConfig.getCodec().isTransforming()) {
      return this.fieldConfig.getCodec().encode(this.literal);
    } else
      throw new ScanException("cannot create fuzzy scan literal " + "for "
          + this.fieldConfig.getCodecType() + " encoded key field with path '"
          + this.fieldConfig.getPropertyPath() + "' within table " + this.table.getName()
          + " for graph root type, " + this.rootType.toString());
  }

  @Override
  public byte[] getFuzzyInfoBytes() {
    byte[] infoBytes = new byte[this.fieldConfig.getMaxLength()];
    Arrays.fill(infoBytes, (byte) 0); // fixed char
    return infoBytes;
  }

  /**
   * Returns the bytes used to represent "equals" relational operator under an
   * HBase row-key 'Get' operation for this string (data flavor) literal under
   * the various optionally configurable hashing, formatting and padding
   * features.
   * 
   * @return the "start row" bytes used to represent "equals" relational
   *         operator under an HBase partial row-key scan for this string (data
   *         flavor) literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  @Override
  public byte[] getEqualsBytes() {
    return getEqualsStartBytes();
  }
}
