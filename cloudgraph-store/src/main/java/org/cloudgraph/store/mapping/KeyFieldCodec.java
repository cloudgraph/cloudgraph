package org.cloudgraph.store.mapping;

/**
 * Responsible for converting composite row key field, actual as well as user
 * supplied literal, query values to and from byte representations.
 * 
 * @author Scott Cinnamond
 * @since 1.1.0
 */
public interface KeyFieldCodec {
  public byte[] encode(byte[] key);

  public byte[] decode(byte[] key);

  public byte[] encodeNext(byte[] key);
}
