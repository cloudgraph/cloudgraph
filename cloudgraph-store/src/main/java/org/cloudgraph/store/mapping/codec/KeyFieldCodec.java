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

/**
 * Implementations are responsible for converting composite row key field,
 * actual as well as user supplied literal, query values to and from byte
 * representations. Bridges the key field creation activity using actual data
 * objects, and the querying of key fields using only meta data describing them.
 * 
 * @author Scott Cinnamond
 * @since 1.1.0
 */
public interface KeyFieldCodec {

  /**
   * Return true if the encoded values support lexicographic ordering. For
   * example integer values are not encoded natively but as the integer string
   * representation.
   * 
   * @return true if the encoded values support lexicographic ordering
   */
  public boolean isLexicographic();

  /**
   * Return true if the encoded values are fundamentally transformed father than
   * formatted or padded in some way.
   * 
   * @return true if the encoded values are fundamentally transformed father
   *         than formatted or padded in some way.
   */
  public boolean isTransforming();

  /**
   * Returns true if the given encoded value will overflow the underlying key
   * field.
   * 
   * @param keyValue
   * @return true if the given encoded value will overflow the underlying key
   *         field.
   */
  public boolean checkEncodeOverflow(byte[] keyValue);

  /**
   * Returns the encoded bytes representation for the given object or literal
   * value.
   * 
   * @param value
   *          the object or literal value
   * @return the encoded bytes representation
   * @throws KeyFieldOverflowException
   *           if the encoded data exceeds the capacity of the target field
   */
  public byte[] encode(Object value);

  /**
   * Returns the object representation for the given encoded bytes value.
   * 
   * @param value
   *          encoded bytes value
   * @return the object representation for the given encoded bytes value
   */
  public Object decode(byte[] value);

  /**
   * Returns the encoded "next" bytes representation for the given object or
   * literal value, where the next value depends on the coded implementation and
   * may typically be the lexicographic next value.
   * 
   * @param value
   *          the object or literal value
   * @return the encoded bytes representation
   * @throws KeyFieldOverflowException
   *           if the encoded data exceeds the capacity of the target field
   */
  public byte[] encodeNext(Object value);
}
