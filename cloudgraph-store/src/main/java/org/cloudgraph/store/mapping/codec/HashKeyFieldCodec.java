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

import org.cloudgraph.common.Bytes;
import org.cloudgraph.common.hash.Hash;
import org.cloudgraph.common.hash.JenkinsHash;
import org.cloudgraph.common.hash.MurmurHash;
import org.cloudgraph.store.mapping.HashAlgorithmName;
import org.cloudgraph.store.mapping.KeyFieldMapping;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;
import org.plasma.sdo.helper.DataConverter;

import commonj.sdo.Type;

/**
 * A non-lexicographic {@link KeyFieldCodec} which emits hash value bytes for
 * the {@link DataType} specific byte representation for the given value.
 * 
 * <p>
 * </p>
 * This codec saves on space by terminating with the minimum added bytes
 * possible for the hashed bytes for the native {@link DataType}.
 * 
 * 
 * @see LexicographicCodec
 * @see Type
 * @see DataFlavor
 * 
 * @author Scott Cinnamond
 * @since 1.1.0
 */
public class HashKeyFieldCodec extends DefaultKeyFieldCodec implements KeyFieldCodec {
  protected HashAlgorithmName hashName;
  protected Hash hash;

  @Override
  public boolean isLexicographic() {
    return false;
  }

  @Override
  public boolean isTransforming() {
    return true;
  }

  public HashKeyFieldCodec(KeyFieldMapping keyField, HashAlgorithmName hash) {
    super(keyField);
    this.hashName = hash;
    switch (this.hashName) {
    case JENKINS:
      this.hash = JenkinsHash.instance();
      break;
    case MURMUR:
      this.hash = MurmurHash.instance();
      break;
    default:
      throw new IllegalArgumentException("unknown hash, " + this.hashName);
    }
  }

  public Hash getHash() {
    return hash;
  }

  @Override
  public byte[] encode(Object value) {
    byte[] bytesValue = DataConverter.INSTANCE.toBytes(this.keyField.getDataType(), value);
    byte[] hashedBytesValue = Bytes.toBytes(Integer.valueOf(hash.hash(bytesValue)));
    return hashedBytesValue;
  }

  @Override
  public boolean checkEncodeOverflow(byte[] keyValue) {
    int delta = this.keyField.getMaxLength() - keyValue.length;
    if (delta < 0)
      return true;
    return false;
  }

  @Override
  public Object decode(byte[] value) {
    throw new CodecException("operation not supported for codec - cannont decode a one-way hash");
  }

  @Override
  public byte[] encodeNext(Object value) {
    throw new CodecException("operation not supported for non-lexicographic codec");
  }

  // @Override
  // public byte[] writeEqualsStartBytes(String key) {
  // byte[] bytes = key.getBytes(charset);
  // return Bytes.toBytes(Integer.valueOf(hash.hash(bytes)));
  // }
  //
  // @Override
  // public byte[] writeEqualsStopBytes(String key) {
  // byte[] bytes = key.getBytes(charset);
  // int hashedValue = Integer.valueOf(hash.hash(bytes));
  // hashedValue++;
  // return Bytes.toBytes(hashedValue);
  // }
  //
}
