package org.cloudgraph.store.mapping;

import org.plasma.sdo.DataFlavor;

public class NativeKeyFieldCodec implements KeyFieldCodec {
  private DataFlavor dataFlavor;

  public NativeKeyFieldCodec(DataFlavor dataFlavor) {
    super();
    this.dataFlavor = dataFlavor;
  }

  @Override
  public byte[] encode(byte[] key) {
    return key;
  }

  @Override
  public byte[] encodeNext(byte[] key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] decode(byte[] key) {
    // TODO Auto-generated method stub
    return null;
  }

  public DataFlavor getDataFlavor() {
    return dataFlavor;
  }

}
