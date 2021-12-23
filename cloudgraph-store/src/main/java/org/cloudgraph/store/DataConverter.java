package org.cloudgraph.store;

import commonj.sdo.Property;

public interface DataConverter {

  /**
   * Converts the given value from bytes based on data type and cardinality
   * information for the given property. For 'many' or multi-valued properties,
   * if the SDO Java type for the property is not already String, the value is
   * first converted from a String using the SDO conversion which uses
   * java.util.Arrays formatting, resulting in an array of primitive types. For
   * non 'many' or singular properties, only the HBase Bytes utility is used.
   * 
   * @param targetProperty
   *          the property
   * @param value
   *          the bytes value
   * @return the converted object
   * @throws IllegalArgumentException
   *           if the given property is not a data type property
   */
  public Object fromBytes(Property targetProperty, byte[] value);

  /**
   * Converts the given value to bytes based on data type and cardinality
   * information for the given property. For 'many' or multi-valued properties,
   * if the SDO Java type for the property is not already String, the value is
   * first converted to String using the SDO conversion which uses
   * java.util.Arrays formatting. For non 'many' or singular properties, only
   * the HBase Bytes utility is used.
   * 
   * @param sourceProperty
   *          the source property
   * @param value
   *          the value
   * @return the bytes
   * @throws IllegalArgumentException
   *           if the given property is not a data type property
   */
  public byte[] toBytes(Property sourceProperty, Object value);

}