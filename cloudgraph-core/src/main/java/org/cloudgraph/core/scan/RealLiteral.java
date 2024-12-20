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
package org.cloudgraph.core.scan;

import java.math.BigDecimal;
import java.util.Arrays;

import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.plasma.query.model.LogicalOperatorName;
import org.plasma.query.model.RelationalOperatorName;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Type;

/**
 * A real data "flavor" specific literal class used to abstract the complexities
 * involved in assembling the various segments and fields of composite (scan
 * start/stop) row keys under various relational and logical operator and
 * various configurable composite key-field hashing, formatting, padding and
 * other features. A real literal does not contain or involve wildcards but
 * nevertheless may "participate" in a fuzzy scan as part of a composite row key
 * and therefore implements {@link FuzzyRowKeyLiteral} supplying only default
 * key and info bytes.
 * 
 * 
 * @see org.cloudgraph.store.mapping.TableMapping
 * @see org.cloudgraph.hbase.service.HBaseDataConverter
 * @author Scott Cinnamond
 * @since 0.5
 */
public class RealLiteral extends ScanLiteral implements PartialRowKeyLiteral, FuzzyRowKeyLiteral,
    CompleteRowKeyLiteral {

  public static final float INCREMENT_FLOAT = Float.MIN_VALUE;
  public static final double INCREMENT_DOUBLE = Double.MIN_VALUE;
  public static final BigDecimal INCREMENT_DECIMAL = BigDecimal.valueOf(Double.MIN_VALUE);

  public RealLiteral(String literal, PlasmaType rootType,
      RelationalOperatorName relationalOperator, LogicalOperatorName logicalOperatorContext,
      DataRowKeyFieldMapping fieldConfig, ServiceContext serviceContext) {
    super(literal, rootType, relationalOperator, logicalOperatorContext, fieldConfig,
        serviceContext);
  }

  /**
   * Returns the "start row" bytes used to represent "equals" relational
   * operator under an HBase partial row-key scan for this real (data flavor)
   * literal under the various optionally configurable hashing, formatting and
   * padding features.
   * 
   * @return the "start row" bytes used to represent "equals" relational
   *         operator under an HBase partial row-key scan for this real (data
   *         flavor) literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getEqualsStartBytes() {
    return this.literalToBytes();
  }

  /**
   * Returns the "stop row" bytes used to represent "equals" relational operator
   * under an HBase partial row-key scan for this real (data flavor) literal
   * under the various optionally configurable hashing, formatting and padding
   * features.
   * 
   * @return the "stop row" bytes used to represent "equals" relational operator
   *         under an HBase partial row-key scan for this real (data flavor)
   *         literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getEqualsStopBytes() {
    return this.nextLiteralToBytes();
  }

  /**
   * Returns the "start row" bytes used to represent "greater than" relational
   * operator under an HBase partial row-key scan for this real (data flavor)
   * literal under the various optionally configurable hashing, formatting and
   * padding features.
   * 
   * @return the "start row" bytes used to represent "greater than" relational
   *         operator under an HBase partial row-key scan for this real (data
   *         flavor) literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getGreaterThanStartBytes() {
    return this.nextLiteralToBytes();
  }

  /**
   * Returns the "stop row" bytes used to represent "greater than" relational
   * operator under an HBase partial row-key scan for this real (data flavor)
   * literal under the various optionally configurable hashing, formatting and
   * padding features.
   * 
   * @return the "stop row" bytes used to represent "greater than" relational
   *         operator under an HBase partial row-key scan for this real (data
   *         flavor) literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getGreaterThanStopBytes() {
    return new byte[0];
  }

  /**
   * Returns the "start row" bytes used to represent "greater than equals"
   * relational operator under an HBase partial row-key scan for this real (data
   * flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "greater than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         real (data flavor) literal under the various optionally
   *         configurable hashing, formatting and padding features.
   */
  public byte[] getGreaterThanEqualStartBytes() {
    return this.literalToBytes();
  }

  /**
   * Returns the "stop row" bytes used to represent "greater than equals"
   * relational operator under an HBase partial row-key scan for this real (data
   * flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "greater than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         real (data flavor) literal under the various optionally
   *         configurable hashing, formatting and padding features.
   */
  public byte[] getGreaterThanEqualStopBytes() {
    return new byte[0];
  }

  /**
   * Returns the "start row" bytes used to represent "less than" relational
   * operator under an HBase partial row-key scan for this real (data flavor)
   * literal under the various optionally configurable hashing, formatting and
   * padding features.
   * 
   * @return the "start row" bytes used to represent "less than" relational
   *         operator under an HBase partial row-key scan for this real (data
   *         flavor) literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getLessThanStartBytes() {
    return new byte[0];
  }

  /**
   * Returns the "stop row" bytes used to represent "less than" relational
   * operator under an HBase partial row-key scan for this real (data flavor)
   * literal under the various optionally configurable hashing, formatting and
   * padding features.
   * 
   * @return the "stop row" bytes used to represent "less than" relational
   *         operator under an HBase partial row-key scan for this real (data
   *         flavor) literal under the various optionally configurable hashing,
   *         formatting and padding features.
   */
  public byte[] getLessThanStopBytes() {
    // Note: in HBase the stop row is exclusive, so just use
    // the literal value, no need to decrement it
    return this.literalToBytes();
  }

  /**
   * Returns the "start row" bytes used to represent "less than equals"
   * relational operator under an HBase partial row-key scan for this real (data
   * flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "start row" bytes used to represent "less than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         real (data flavor) literal under the various optionally
   *         configurable hashing, formatting and padding features.
   */
  public byte[] getLessThanEqualStartBytes() {
    return new byte[0];
  }

  /**
   * Returns the "stop row" bytes used to represent "less than equals"
   * relational operator under an HBase partial row-key scan for this real (data
   * flavor) literal under the various optionally configurable hashing,
   * formatting and padding features.
   * 
   * @return the "stop row" bytes used to represent "less than equals"
   *         relational operator under an HBase partial row-key scan for this
   *         real (data flavor) literal under the various optionally
   *         configurable hashing, formatting and padding features.
   */
  public byte[] getLessThanEqualStopBytes() {
    return this.nextLiteralToBytes();
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
    if (this.fieldMapping.getCodec().isLexicographic()
        && !this.fieldMapping.getCodec().isTransforming())
      return getEqualsStartBytes();
    else
      throw new ScanException("cannot create fuzzy scan literal "
          + "for "
          + this.fieldMapping.getCodecType()
          + " encoded key field with path '"
          + this.fieldMapping.getPropertyPath()
          + "' within table "
          + this.serviceContext.getNamespaceQualifiedPhysicalName(this.table,
              this.serviceContext.getStoreMapping()) + " for graph root type, "
          + this.rootType.toString());
  }

  @Override
  public byte[] getFuzzyInfoBytes() {
    byte[] infoBytes = new byte[this.fieldMapping.getMaxLength()];
    Arrays.fill(infoBytes, (byte) 0); // fixed char
    return infoBytes;
  }

  private byte[] literalToBytes() {
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    return this.fieldMapping.getCodec().encode(value);
  }

  private byte[] nextLiteralToBytes() {
    Object value = this.dataConverter.convert(property.getType(), this.literal);
    return this.fieldMapping.getCodec().encodeNext(value);
  }

  private String increment(Type type, Object value) {
    String result = "";
    DataType sourceDataType = DataType.valueOf(type.getName());
    switch (sourceDataType) {
    case Float:
      Float floatValue = this.dataConverter.toFloat(property.getType(), value);
      int intBits = Float.floatToRawIntBits(floatValue.floatValue());
      intBits++;
      Float floatResult = Float.valueOf(Float.intBitsToFloat(intBits));
      result = this.dataConverter.toString(type, floatResult);
      break;
    case Double:
      Double doubleValue = this.dataConverter.toDouble(property.getType(), value);
      long longBits = Double.doubleToRawLongBits(doubleValue.doubleValue());
      longBits++;
      Double doubleResult = Double.valueOf(Double.longBitsToDouble(longBits));
      result = this.dataConverter.toString(type, doubleResult);
      break;
    case Decimal:
      BigDecimal decimalValue = this.dataConverter.toDecimal(property.getType(), value);
      // FIXME: loss of precision
      double temp = decimalValue.doubleValue();
      longBits = Double.doubleToRawLongBits(temp);
      longBits++;
      doubleResult = Double.valueOf(Double.longBitsToDouble(longBits));
      result = this.dataConverter.toString(type, doubleResult);
      break;
    default:
      throw new ScanException("expected real (Float, Double, Decinal)datatype not, "
          + sourceDataType.name());
    }
    return result;
  }

}
