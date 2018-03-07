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

import org.cloudgraph.common.Padding;
import org.cloudgraph.store.key.KeyFieldOverflowException;
import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.helper.DataConverter;

import commonj.sdo.Type;

/**
 * A lexicographic {@link KeyFieldCodec} which uses {@link DataFlavor} specific
 * padding to force the encoded result to the limit or given maximum length,
 * creating fixed length key fields important in many partial and fuzzy scan
 * scenarios.
 * 
 * <p>
 * </p>
 * This codec can cause greatly increased data sizes where given max length far
 * exceeds the length of the actual data to encode.
 * 
 * 
 * @see LexicographicCodec
 * @see Type
 * @see DataFlavor
 * @see Padding
 * 
 * @author Scott Cinnamond
 * @since 1.1.0
 */
public class PaddingKeyFieldCodec implements KeyFieldCodec {
  private KeyFieldMapping keyField;
  private Padding padding;

  @SuppressWarnings("unused")
  private PaddingKeyFieldCodec() {
  }

  public PaddingKeyFieldCodec(KeyFieldMapping keyField) {
    super();
    this.keyField = keyField;
    this.padding = new Padding(this.keyField.CHARSET);
  }

  @Override
  public boolean isLexicographic() {
    return true;
  }

  @Override
  public boolean isTransforming() {
    return false;
  }

  public int getMaxLength() {
    return this.keyField.getMaxLength();
  }

  public DataFlavor getDataFlavor() {
    return this.keyField.getDataFlavor();
  }

  @Override
  public byte[] encode(Object value) throws KeyFieldOverflowException {
    String stringValue = DataConverter.INSTANCE.toString(this.keyField.getDataType(), value);
    byte[] bytesValue = stringValue.getBytes(this.keyField.CHARSET);
    int delta = this.getMaxLength() - bytesValue.length;
    if (delta < 0)
      throw new KeyFieldOverflowException("value '" + stringValue
          + "' exceeds capacity for key field: " + this.keyField);

    return this.padding.pad(bytesValue, this.getMaxLength(), this.getDataFlavor());
  }

  @Override
  public Object decode(byte[] value) {
    byte[] value2 = value;
    if (value[value.length - 1] == Byte.MIN_VALUE) {
      value2 = new byte[value.length - 1];
      System.arraycopy(value, 0, value2, 0, value.length - 1);
    }
    byte[] bytesValue = this.padding.strip(value2, this.getMaxLength(), this.getDataFlavor());
    String stringValue = new String(bytesValue, this.keyField.CHARSET);
    Object result = DataConverter.INSTANCE.fromString(this.keyField.getDataType(), stringValue);
    return result;
  }

  @Override
  public byte[] encodeNext(Object value) throws KeyFieldOverflowException {
    String stringValue = DataConverter.INSTANCE.toString(this.keyField.getDataType(), value);
    byte[] bytesValue = stringValue.getBytes(this.keyField.CHARSET);
    int delta = this.getMaxLength() - bytesValue.length;
    if (delta < 0)
      throw new KeyFieldOverflowException("value '" + stringValue
          + "' exceeds capacity for key field: " + this.keyField);
    byte[] paddedValue = this.padding.pad(bytesValue, this.getMaxLength(), this.getDataFlavor());
    byte[] result = new byte[paddedValue.length + 1];
    System.arraycopy(paddedValue, 0, result, 0, paddedValue.length);
    result[result.length - 1] = Byte.MIN_VALUE;
    return result;
  }

  // @Override
  // public byte[] writeEqualsStartBytes(String key) {
  // byte[] result = key.getBytes(this.charset);
  // return this.padding.pad(result, this.maxLength, this.dataFlavor);
  // }
  //
  // @Override
  // public byte[] writeEqualsStopBytes(String key) {
  // byte[] padded = key.getBytes(this.charset);
  // padded = this.padding.pad(padded, this.maxLength, this.dataFlavor);
  // byte[] result = new byte[padded.length+1];
  // System.arraycopy(result, 0, padded, 0, padded.length);
  // result[padded.length] = Byte.MIN_VALUE;
  // return result;
  // }

}
