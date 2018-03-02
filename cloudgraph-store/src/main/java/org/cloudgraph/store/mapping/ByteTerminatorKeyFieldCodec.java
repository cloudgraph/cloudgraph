package org.cloudgraph.store.mapping;

import org.plasma.sdo.DataFlavor;

public class ByteTerminatorKeyFieldCodec implements KeyFieldCodec {
  private DataFlavor dataFlavor;

  public ByteTerminatorKeyFieldCodec(DataFlavor dataFlavor) {
    super();
    this.dataFlavor = dataFlavor;
  }

  @Override
  public byte[] encode(byte[] key) {
    byte[] result = new byte[key.length + 1];
    System.arraycopy(key, 0, result, 0, key.length);
    result[key.length] = Byte.MIN_VALUE;
    return result;
  }

  @Override
  public byte[] encodeNext(byte[] key) {
    byte[] result = new byte[key.length + 1];
    System.arraycopy(key, 0, result, 0, key.length);
    result[key.length] = Byte.MIN_VALUE + 1;
    return result;
  }

  @Override
  public byte[] decode(byte[] key) {
    byte[] result = new byte[key.length - 1];
    System.arraycopy(key, 0, result, 0, key.length - 1);
    return result;
  }

  public DataFlavor getDataFlavor() {
    return dataFlavor;
  }

}
