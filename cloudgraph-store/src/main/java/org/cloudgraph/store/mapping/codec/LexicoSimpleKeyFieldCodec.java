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

import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.helper.DataConverter;

import commonj.sdo.Type;

/**
 * A lexicographic {@link KeyFieldCodec} which simply encodes data as a
 * lexicographic string value.
 * 
 * 
 * @see Type
 * @see DataFlavor
 * 
 * @author Scott Cinnamond
 * @since 1.1.0
 */
public class LexicoSimpleKeyFieldCodec extends DefaultKeyFieldCodec implements KeyFieldCodec {

  public LexicoSimpleKeyFieldCodec(KeyFieldMapping keyField) {
    super(keyField);
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
  public byte[] encode(Object value) {
    String stringValue = DataConverter.INSTANCE.toString(keyField.getDataType(), value);
    byte[] result = stringValue.getBytes(keyField.CHARSET);
    return result;
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
    String stringValue = DataConverter.INSTANCE.toString(keyField.getDataType(), value);
    byte[] bytesValue = stringValue.getBytes(keyField.CHARSET);
    byte[] result = new byte[bytesValue.length + 1];
    System.arraycopy(bytesValue, 0, result, 0, bytesValue.length);
    result[bytesValue.length] = Byte.MIN_VALUE;
    return result;
  }

  @Override
  public Object decode(byte[] value) {
    String stringResult = new String(value, keyField.CHARSET);
    Object result = DataConverter.INSTANCE.fromString(keyField.getDataType(), stringResult);
    return result;
  }

  public DataFlavor getDataFlavor() {
    return keyField.getDataFlavor();
  }

}
