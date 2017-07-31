package sorts;

import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * InsertionSort.java
 * Created by Stijn Strickx on May 21, 2008
 * Copyright 2008 Stijn Strickx, All rights reserved
 */

/**
 * Insertion sort algorithm Time Complexity: O(n*n) Memory Complexity: O(1)
 * Stable: yes
 */

public class InsertionSort extends Sorter {
  private static Log log = LogFactory.getFactory().getInstance(InsertionSort.class);

  @Override
  public <T extends Comparable<? super T>> void sort(T[] a) {
    for (int i = 1; i < a.length; i++) {
      int j = i;
      T o = a[i];
      while ((j > 0) && o.compareTo(a[j - 1]) < 0) {
        a[j] = a[j - 1];
        j--;
      }
      a[j] = o;
    }
  }

  public <T> void sort(T[] a, Comparator<T> c) {
    for (int i = 1; i < a.length; i++) {
      int j = i;
      T o = a[i];
      while ((j > 0) && c.compare(o, a[j - 1]) < 0) {
        if (log.isDebugEnabled())
          log.debug("swapping (" + String.valueOf(j) + ") " + a[j].toString() + " for ("
              + String.valueOf(j - 1) + ") " + a[j - 1].toString());
        a[j] = a[j - 1];
        j--;
      }
      a[j] = o;
    }
  }
}