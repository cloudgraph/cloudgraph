package org.cloudgraph.store.mapping;

import java.nio.charset.Charset;

import org.cloudgraph.common.Padding;
import org.plasma.sdo.DataFlavor;

public class PaddingKeyFieldCodec implements KeyFieldCodec {
  private Charset charset;
  private int maxLength;
  private DataFlavor dataFlavor;
  private Padding padding;

  @SuppressWarnings("unused")
  private PaddingKeyFieldCodec() {
  }

  public PaddingKeyFieldCodec(Charset charset, int maxLength, DataFlavor dataFlavor) {
    super();
    this.charset = charset;
    this.maxLength = maxLength;
    this.dataFlavor = dataFlavor;
    this.padding = new Padding(this.charset);
  }

  public Charset getCharset() {
    return charset;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public DataFlavor getDataFlavor() {
    return dataFlavor;
  }

  @Override
  public byte[] encode(byte[] key) {
    return this.padding.pad(key, this.maxLength, this.dataFlavor);
  }

  @Override
  public byte[] decode(byte[] key) {
    return this.padding.strip(key, this.maxLength, this.dataFlavor);
  }

  @Override
  public byte[] encodeNext(byte[] key) {
    byte[] value = this.padding.pad(key, this.maxLength, this.dataFlavor);
    byte[] result = new byte[value.length + 1];
    System.arraycopy(value, 0, result, 0, value.length);
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
