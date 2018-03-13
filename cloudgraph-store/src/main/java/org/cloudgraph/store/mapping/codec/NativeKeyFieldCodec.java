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
package org.cloudgraph.store.mapping.codec;

import java.math.BigInteger;

import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;
import org.plasma.sdo.helper.DataConverter;

import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;

import commonj.sdo.Type;

/**
 * A non-lexicographic {@link KeyFieldCodec} which emits the {@link DataType}
 * specific native byte representation for the given value. This codec cannot be
 * used for composite row keys, as the native encoding of say integral types may
 * easily result in, for example one byte out of a 4-byte int being the same
 * byte/char as the declared delimiter field. Support for certain binary
 * compatible delimiter characters like '\0' should be explored.
 * 
 * <p>
 * </p>
 * This codec saves on space by terminating with the minimum added bytes
 * possible for the native {@link DataType}.
 * 
 * 
 * @see Type
 * @see DataFlavor
 * 
 * @author Scott Cinnamond
 * @since 1.1.0
 */
public class NativeKeyFieldCodec extends DefaultKeyFieldCodec implements KeyFieldCodec {
  private static final short INCREMENT_INTEGRAL = 1;

  public NativeKeyFieldCodec(KeyFieldMapping keyField) {
    super(keyField);
    this.keyField = keyField;
  }

  @Override
  public boolean isLexicographic() {
    return false;
  }

  @Override
  public boolean isTransforming() {
    return true;
  }

  @Override
  public byte[] encode(Object value) {
    byte[] bytesValue = DataConverter.INSTANCE.toBytes(this.keyField.getDataType(), value);
    return bytesValue;
  }

  @Override
  public boolean checkEncodeOverflow(byte[] keyValue) {
    int delta = this.keyField.getMaxLength() - keyValue.length;
    if (delta < 0)
      return true;
    return false;
  }

  @Override
  public byte[] encodeNext(Object value) {
    throw new CodecException("operation not supported for non-lexicographic codec");
  }

  @Override
  public Object decode(byte[] value) {
    return DataConverter.INSTANCE.fromBytes(this.keyField.getDataType(), value);
  }

  public DataFlavor getDataFlavor() {
    return this.keyField.getDataFlavor();
  }

  private Object increment(Object value) {
    Object result = null;
    switch (this.keyField.getDataType()) {
    case Short:
      Short shortValue = DataConverter.INSTANCE.toShort(DataType.Short, value);
      result = Short.valueOf((short) (shortValue.shortValue() + INCREMENT_INTEGRAL));
      break;
    case Int:
      Integer intValue = DataConverter.INSTANCE.toInt(DataType.Int, value);
      result = Integer.valueOf(intValue.intValue() + INCREMENT_INTEGRAL);
      break;
    case UnsignedInt:
      UnsignedInteger uintValue = DataConverter.INSTANCE.toUnsignedInt(DataType.UnsignedInt, value);
      result = UnsignedInteger.valueOf(uintValue.longValue() + INCREMENT_INTEGRAL);
      break;
    case Long:
      Long longValue = DataConverter.INSTANCE.toLong(DataType.Long, value);
      result = Long.valueOf(longValue.longValue() + INCREMENT_INTEGRAL);
      break;
    case UnsignedLong:
      UnsignedLong ulongValue = DataConverter.INSTANCE.toUnsignedLong(DataType.UnsignedLong, value);
      BigInteger bigIntValue = ulongValue.bigIntegerValue();
      bigIntValue = bigIntValue.add(BigInteger.valueOf(INCREMENT_INTEGRAL));
      result = UnsignedLong.valueOf(bigIntValue);
      break;
    case Integer:
      BigInteger integerValue = DataConverter.INSTANCE.toInteger(DataType.Integer, value);
      result = integerValue.add(BigInteger.valueOf(INCREMENT_INTEGRAL));
      break;
    default:
      throw new CodecException("expected integral datatype not, "
          + this.keyField.getDataType().name());
    }
    return result;
  }
}
