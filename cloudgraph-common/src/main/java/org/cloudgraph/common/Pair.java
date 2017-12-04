package org.cloudgraph.common;

public class Pair<U, V> {
  private U left;
  private V right;

  @SuppressWarnings("unused")
  private Pair() {
    super();
  }

  public Pair(U left, V right) {
    super();
    this.left = left;
    this.right = right;
  }

  public U getLeft() {
    return left;
  }

  public V getRight() {
    return right;
  }

}
