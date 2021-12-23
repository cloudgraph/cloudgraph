package sorts;

/**
 * BubbleSort.java
 * Created by Stijn Strickx on May 21, 2008
 * Copyright 2008 Stijn Strickx, All rights reserved
 */

/**
 * Bubble sort algorithm Time Complexity: O(n*n) Memory Complexity: O(1) Stable:
 * yes
 */

public class BubbleSort extends Sorter {

  @Override
  public <T extends Comparable<? super T>> void sort(T[] a) {
    boolean swapped = true;
    int i = a.length - 1;
    while (swapped && i >= 0) {
      swapped = false;
      for (int j = 0; j < i; j++) {
        if (a[j].compareTo(a[j + 1]) > 0) {
          swap(a, j, j + 1);
          swapped = true;
        }
      }
      i--;
    }
  }
}