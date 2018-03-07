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

import org.cloudgraph.store.key.KeyFieldOverflowException;
import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.helper.DataConverter;

import commonj.sdo.Type;

/**
 * A lexicographic {@link KeyFieldCodec} which simply terminates the encoded
 * data with the lexicographic minimum value.
 * 
 * <p>
 * </p>
 * This codec saves on space by terminating with the minimum added bytes
 * possible.
 * 
 * 
 * @see LexicographicCodec
 * @see Type
 * @see DataFlavor
 * 
 * @author Scott Cinnamond
 * @since 1.1.0
 */
public class MinBytesKeyFieldCodec implements KeyFieldCodec {
  private KeyFieldMapping keyField;

  public MinBytesKeyFieldCodec(KeyFieldMapping keyField) {
    super();
    this.keyField = keyField;
  }

  @Override
  public boolean isLexicographic() {
    return true;
  }

  @Override
  public boolean isTransforming() {
    return false;
  }

  @Override
  public byte[] encode(Object value) throws KeyFieldOverflowException {
    String stringValue = DataConverter.INSTANCE.toString(keyField.getDataType(), value);
    byte[] bytesValue = stringValue.getBytes(keyField.CHARSET);
    int delta = this.keyField.getMaxLength() - bytesValue.length;
    if (delta < 0)
      throw new KeyFieldOverflowException("value '" + stringValue
          + "' exceeds capacity for key field: " + this.keyField);
    byte[] result = new byte[bytesValue.length + 1];
    System.arraycopy(value, 0, result, 0, bytesValue.length);
    result[bytesValue.length] = Byte.MIN_VALUE;
    return result;
  }

  @Override
  public byte[] encodeNext(Object value) throws KeyFieldOverflowException {
    String stringValue = DataConverter.INSTANCE.toString(keyField.getDataType(), value);
    byte[] bytesValue = stringValue.getBytes(keyField.CHARSET);
    int delta = this.keyField.getMaxLength() - bytesValue.length;
    if (delta < 0)
      throw new KeyFieldOverflowException("value '" + stringValue
          + "' exceeds capacity for key field: " + this.keyField);
    byte[] result = new byte[bytesValue.length + 1];
    System.arraycopy(bytesValue, 0, result, 0, bytesValue.length);
    // one after lexicographic min byte
    result[bytesValue.length] = Byte.MIN_VALUE + 1;
    return result;
  }

  @Override
  public Object decode(byte[] value) {
    byte[] bytesResult = new byte[value.length - 1];
    System.arraycopy(value, 0, bytesResult, 0, value.length - 1);
    String stringResult = new String(bytesResult, keyField.CHARSET);
    Object result = DataConverter.INSTANCE.fromString(keyField.getDataType(), stringResult);
    return result;
  }

  public DataFlavor getDataFlavor() {
    return keyField.getDataFlavor();
  }

}
