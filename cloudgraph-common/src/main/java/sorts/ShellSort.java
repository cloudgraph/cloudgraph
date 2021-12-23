package sorts;

/**
 * ShellSort.java
 * Created by Stijn Strickx on May 21, 2008
 * Copyright 2008 Stijn Strickx, All rights reserved
 */

/**
 * Shell sort algorithm Time Complexity: O(n log2 n) Memory Complexity: O(1)
 * Stable: no
 */

public class ShellSort extends Sorter {

  @Override
  public <T extends Comparable<? super T>> void sort(T[] a) {
    int h = 1;
    while ((h * 3 + 1) < a.length)
      h = 3 * h + 1;
    while (h > 0) {
      for (int i = h - 1; i < a.length; i++) {
        T s = a[i];
        int j = i;
        for (j = i; (j >= h) && (a[j - h].compareTo(s) > 0); j -= h)
          a[j] = a[j - h];
        a[j] = s;
      }
      h /= 3;
    }
  }
}