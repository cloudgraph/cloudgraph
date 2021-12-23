package sorts;

/**
 * Sort.java
 * Created by Stijn Strickx on May 21, 2008
 * Copyright 2008 Stijn Strickx, All rights reserved
 */

/**
 * These algorithms can be used to sort an array of any primitive datatype /
 * Comparable Object. The datatype conversions for the arrays won't influence
 * the time complexity of the sorting algorithms. Without these conversions, the
 * algorithms would have to be re-written for every primitive datatype.
 */

public class Sort {

  public static <T extends Comparable<? super T>> void bubbleSort(T[] a) {
    (new BubbleSort()).sort(a);
  }

  public static int[] bubbleSort(int[] a) {
    Integer[] b = OArrays.toObjectArray(a);
    bubbleSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static long[] bubbleSort(long[] a) {
    Long[] b = OArrays.toObjectArray(a);
    bubbleSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static double[] bubbleSort(double[] a) {
    Double[] b = OArrays.toObjectArray(a);
    bubbleSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static float[] bubbleSort(float[] a) {
    Float[] b = OArrays.toObjectArray(a);
    bubbleSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static char[] bubbleSort(char[] a) {
    Character[] b = OArrays.toObjectArray(a);
    bubbleSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static boolean[] bubbleSort(boolean[] a) {
    Boolean[] b = OArrays.toObjectArray(a);
    bubbleSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static byte[] bubbleSort(byte[] a) {
    Byte[] b = OArrays.toObjectArray(a);
    bubbleSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static <T extends Comparable<? super T>> void selectionSort(T[] a) {
    (new SelectionSort()).sort(a);
  }

  public static int[] selectionSort(int[] a) {
    Integer[] b = OArrays.toObjectArray(a);
    selectionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static long[] selectionSort(long[] a) {
    Long[] b = OArrays.toObjectArray(a);
    selectionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static double[] selectionSort(double[] a) {
    Double[] b = OArrays.toObjectArray(a);
    selectionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static float[] selectionSort(float[] a) {
    Float[] b = OArrays.toObjectArray(a);
    selectionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static char[] selectionSort(char[] a) {
    Character[] b = OArrays.toObjectArray(a);
    selectionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static boolean[] selectionSort(boolean[] a) {
    Boolean[] b = OArrays.toObjectArray(a);
    selectionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static byte[] selectionSort(byte[] a) {
    Byte[] b = OArrays.toObjectArray(a);
    selectionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static <T extends Comparable<? super T>> void insertionSort(T[] a) {
    (new InsertionSort()).sort(a);
  }

  public static int[] insertionSort(int[] a) {
    Integer[] b = OArrays.toObjectArray(a);
    insertionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static long[] insertionSort(long[] a) {
    Long[] b = OArrays.toObjectArray(a);
    insertionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static double[] insertionSort(double[] a) {
    Double[] b = OArrays.toObjectArray(a);
    insertionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static float[] insertionSort(float[] a) {
    Float[] b = OArrays.toObjectArray(a);
    insertionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static char[] insertionSort(char[] a) {
    Character[] b = OArrays.toObjectArray(a);
    insertionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static boolean[] insertionSort(boolean[] a) {
    Boolean[] b = OArrays.toObjectArray(a);
    insertionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static byte[] insertionSort(byte[] a) {
    Byte[] b = OArrays.toObjectArray(a);
    insertionSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static <T extends Comparable<? super T>> void shellSort(T[] a) {
    (new ShellSort()).sort(a);
  }

  public static int[] shellSort(int[] a) {
    Integer[] b = OArrays.toObjectArray(a);
    shellSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static long[] shellSort(long[] a) {
    Long[] b = OArrays.toObjectArray(a);
    shellSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static double[] shellSort(double[] a) {
    Double[] b = OArrays.toObjectArray(a);
    shellSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static float[] shellSort(float[] a) {
    Float[] b = OArrays.toObjectArray(a);
    shellSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static char[] shellSort(char[] a) {
    Character[] b = OArrays.toObjectArray(a);
    shellSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static boolean[] shellSort(boolean[] a) {
    Boolean[] b = OArrays.toObjectArray(a);
    shellSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static byte[] shellSort(byte[] a) {
    Byte[] b = OArrays.toObjectArray(a);
    shellSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static <T extends Comparable<? super T>> void mergeSort(T[] a) {
    (new MergeSort()).sort(a);
  }

  public static int[] mergeSort(int[] a) {
    Integer[] b = OArrays.toObjectArray(a);
    mergeSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static long[] mergeSort(long[] a) {
    Long[] b = OArrays.toObjectArray(a);
    mergeSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static double[] mergeSort(double[] a) {
    Double[] b = OArrays.toObjectArray(a);
    mergeSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static float[] mergeSort(float[] a) {
    Float[] b = OArrays.toObjectArray(a);
    mergeSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static char[] mergeSort(char[] a) {
    Character[] b = OArrays.toObjectArray(a);
    mergeSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static boolean[] mergeSort(boolean[] a) {
    Boolean[] b = OArrays.toObjectArray(a);
    mergeSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static byte[] mergeSort(byte[] a) {
    Byte[] b = OArrays.toObjectArray(a);
    mergeSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static <T extends Comparable<? super T>> void quickSort(T[] a) {
    (new QuickSort()).sort(a);
  }

  public static int[] quickSort(int[] a) {
    Integer[] b = OArrays.toObjectArray(a);
    quickSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static long[] quickSort(long[] a) {
    Long[] b = OArrays.toObjectArray(a);
    quickSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static double[] quickSort(double[] a) {
    Double[] b = OArrays.toObjectArray(a);
    quickSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static float[] quickSort(float[] a) {
    Float[] b = OArrays.toObjectArray(a);
    quickSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static char[] quickSort(char[] a) {
    Character[] b = OArrays.toObjectArray(a);
    quickSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static boolean[] quickSort(boolean[] a) {
    Boolean[] b = OArrays.toObjectArray(a);
    quickSort(b);
    return OArrays.toPrimitiveArray(b);
  }

  public static byte[] quickSort(byte[] a) {
    Byte[] b = OArrays.toObjectArray(a);
    quickSort(b);
    return OArrays.toPrimitiveArray(b);
  }
}