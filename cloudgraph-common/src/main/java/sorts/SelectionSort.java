package sorts;

/**
 * SelectionSort.java
 * Created by Stijn Strickx on May 21, 2008
 * Copyright 2008 Stijn Strickx, All rights reserved
 */

/**
 * Selection sort algorithm
 * Time Complexity: O(n*n)
 * Memory Complexity: O(1)
 * Stable: yes
 * Note: Other implementations of the selection sort algorithm might not be stable.
 */

public class SelectionSort extends Sorter{

    @Override
    public <T extends Comparable<? super T>> void sort(T[] a) {
        for(int i = 0; i < a.length; i++){
            int min = i;
            for(int j = i+1; j<a.length; j++)
                if(a[j].compareTo(a[min]) < 0)
                    min = j;
            swap(a,min,i);
        }
    }
}