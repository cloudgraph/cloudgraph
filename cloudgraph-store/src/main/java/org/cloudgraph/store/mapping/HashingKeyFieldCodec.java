package org.cloudgraph.store.mapping;

import java.nio.charset.Charset;

import org.cloudgraph.common.Bytes;
import org.cloudgraph.common.hash.Hash;
import org.cloudgraph.common.hash.JenkinsHash;
import org.cloudgraph.common.hash.MurmurHash;

public class HashingKeyFieldCodec implements KeyFieldCodec {
  protected Charset charset;
  protected HashAlgorithmName hashName;
  protected Hash hash;

  @SuppressWarnings("unused")
  private HashingKeyFieldCodec() {
  }

  public HashingKeyFieldCodec(Charset charset, HashAlgorithmName hash) {
    super();
    this.charset = charset;
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

  public Charset getCharset() {
    return charset;
  }

  public Hash getHash() {
    return hash;
  }

  @Override
  public byte[] encode(byte[] key) {
    return Bytes.toBytes(Integer.valueOf(hash.hash(key)));
  }

  @Override
  public byte[] decode(byte[] key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] encodeNext(byte[] key) {
    int hashValue = Integer.valueOf(hash.hash(key));
    hashValue++;
    return Bytes.toBytes(hashValue);
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
