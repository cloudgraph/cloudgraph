/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.hbase.service;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.plasma.sdo.DataType;
import org.plasma.sdo.helper.DataConverter;

import commonj.sdo.Property;

/**
 * HBase specific data converter, delegates to {@link DataConverter} under
 * certain conditions. 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class HBaseDataConverter {
	private static Log log = LogFactory.getFactory().getInstance(
			HBaseDataConverter.class);

	static public volatile HBaseDataConverter INSTANCE = initializeInstance();

	private HBaseDataConverter() {
	}

	/**
	 * Converts the given value to bytes based on data type and cardinality information
	 * for the given property. For 'many' or multi-valued properties, if the SDO Java type
	 * for the property is not already String, the value is first converted to String using
	 * the SDO conversion which uses java.util.Arrays formatting. 
	 * @param sourceProperty the source property
	 * @param value the value
	 * @return the bytes
	 * @throws IllegalArgumentException if the given property is not a data type property 
	 */
	private static synchronized HBaseDataConverter initializeInstance() {
		if (INSTANCE == null)
			INSTANCE = new HBaseDataConverter();
		return INSTANCE;
	}

	/**
	 * Converts the given value from bytes based on data type and cardinality information
	 * for the given property. For 'many' or multi-valued properties, if the SDO Java type
	 * for the property is not already String, the value is first converted from a String using
	 * the SDO conversion which uses java.util.Arrays formatting, resulting in an array of 
	 * primitive types. For non 'many' or singular properties, only the HBase Bytes utility
	 * is used.  
	 * @param targetProperty the property
	 * @param value the bytes value
	 * @return the converted object
	 * @throws IllegalArgumentException if the given property is not a data type property 
	 */
	public Object fromBytes(Property targetProperty, byte[] value)
	{
		Object result = null;
		
		if (!targetProperty.getType().isDataType()) 
            throw new IllegalArgumentException("property " 
                + targetProperty.toString() + " is not a datatype property");  
         
    	DataType targetDataType = DataType.valueOf(targetProperty.getType().getName());
    	
		switch (targetDataType) {
		// Data types stored as String bytes in HBase
		case String:
		case Strings:
		case URI:
		case Month:
		case MonthDay:
		case Day:
		case Time:
		case Year:
		case YearMonth:
		case YearMonthDay:
		case Duration:
			String resultStr = Bytes.toString(value); 
			result = DataConverter.INSTANCE.fromString(targetProperty, resultStr);
			break;
		case Date:
			resultStr = Bytes.toString(value); 
			result = DataConverter.INSTANCE.fromString(targetProperty, resultStr);
			break;
		case DateTime:
			// NOTE: remember datetime is a String Java representation in SDO 2.1
			resultStr = Bytes.toString(value); 
			result = DataConverter.INSTANCE.fromString(targetProperty, resultStr);
			break;
			
	    // Data types stored by directly converting from primitive types to bytes in HBase.
		// TODO: for these data types determine if there is a way to "delimit" multiple values yet
		// not take the extra and expensive step of first converting to delimited String. 
		case Decimal:
			if (!targetProperty.isMany()) 
			    result = Bytes.toBigDecimal(value);
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Bytes: 
			if (!targetProperty.isMany()) 
				result = value; // already bytes 
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Byte:
			if (!targetProperty.isMany()) {
				// NOTE: no toByte method as would expect as there is opposite method, see below
				// e.g. Bytes.toByte(value);
				if (value != null) {
					if (value.length > 2)
						log.warn("truncating "
					        + String.valueOf(value.length) 
					        + " length byte array for target data type 'byte'");
				    result = value[0]; 
				}
			}
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Boolean:
			if (!targetProperty.isMany()) 
				result = Bytes.toBoolean(value);  
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Character:
			if (!targetProperty.isMany()) 
				result = Character.valueOf(Bytes.toString(value).charAt(0));
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Double:
			if (!targetProperty.isMany()) 
				result = Bytes.toDouble(value);   
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Float:
			if (!targetProperty.isMany()) 
				result = Bytes.toFloat(value);  
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Int:
			if (!targetProperty.isMany()) 
				result = Bytes.toInt(value);  
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Integer:
			if (!targetProperty.isMany()) 
				result = new BigInteger(value);
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Long:
			if (!targetProperty.isMany()) 
				result = Bytes.toLong(value);  
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Short:
			if (!targetProperty.isMany()) 
				result = Bytes.toShort(value);  
			else 
				result = DataConverter.INSTANCE.fromString(targetProperty, Bytes.toString(value));
			break;
		case Object:
			// FIXME: custom serialization?
		default:
			result = Bytes.toString(value);  
			break;
		}

		return result;
	}	

	/**
	 * Converts the given value to bytes based on data type and cardinality information
	 * for the given property. For 'many' or multi-valued properties, if the SDO Java type
	 * for the property is not already String, the value is first converted to String using
	 * the SDO conversion which uses java.util.Arrays formatting. For non 'many' or singular 
	 * properties, only the HBase Bytes utility is used.  
	 * @param sourceProperty the source property
	 * @param value the value
	 * @return the bytes
	 * @throws IllegalArgumentException if the given property is not a data type property 
	 */
	public byte[] toBytes(Property sourceProperty, Object value)
	{
		byte[] result;
		
		if (!sourceProperty.getType().isDataType()) 
            throw new IllegalArgumentException("property " 
                + sourceProperty.toString() + " is not a datatype property");  
		DataType dataType = DataType.valueOf(sourceProperty.getType()
				.getName());
		
		switch (dataType) {
		// Data types stored as String bytes in HBase
		case String:
		case Strings:
		case URI:
		case Month:
		case MonthDay:
		case Day:
		case Time:
		case Year:
		case YearMonth:
		case YearMonthDay:
		case Date:
		case Duration:
			String resultStr = DataConverter.INSTANCE.toString(
				sourceProperty, value);
			result = Bytes.toBytes(resultStr);
			break;
		case DateTime:
			resultStr = DataConverter.INSTANCE.toString(
				sourceProperty, value);
			result = Bytes.toBytes(resultStr);
			break;
		// Data types stored by directly converting from primitive types to bytes in HBase. When
		// the given property is a 'many' property, the value is first converted to String so
		// can be delimited.
		case Decimal:
			if (!sourceProperty.isMany()) {
				BigDecimal resultDecimal = DataConverter.INSTANCE.toDecimal(sourceProperty.getType(), value);
				result = Bytes.toBytes(resultDecimal);
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Bytes:
			if (!sourceProperty.isMany()) {
				byte[] resultBytes = DataConverter.INSTANCE.toBytes(sourceProperty.getType(), value);
				result = resultBytes;
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Byte:
			if (!sourceProperty.isMany()) {
				byte resultByte = DataConverter.INSTANCE.toByte(
						sourceProperty.getType(), value);
					result = Bytes.toBytes(resultByte);
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Boolean:
			if (!sourceProperty.isMany()) {
				boolean resultBool = DataConverter.INSTANCE.toBoolean(sourceProperty
						.getType(), value);
				result = Bytes.toBytes(resultBool);
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Character:
			if (!sourceProperty.isMany()) {
				resultStr = DataConverter.INSTANCE.toString(sourceProperty
						.getType(), value);
				result = Bytes.toBytes(resultStr);
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Double:
			if (!sourceProperty.isMany()) {
				double resultDouble = DataConverter.INSTANCE.toDouble(sourceProperty
						.getType(), value);
				result = Bytes.toBytes(resultDouble);
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Float:
			if (!sourceProperty.isMany()) {
				float resultFloat = DataConverter.INSTANCE.toFloat(sourceProperty
						.getType(), value);
				result = Bytes.toBytes(resultFloat);
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Int:
			if (!sourceProperty.isMany()) {
				int resultInt = DataConverter.INSTANCE.toInt(sourceProperty.getType(),
						value);
				result = Bytes.toBytes(resultInt);
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Integer:
			if (!sourceProperty.isMany()) {
				BigInteger resultInteger = DataConverter.INSTANCE.toInteger(sourceProperty
						.getType(), value);
				result = resultInteger.toByteArray();
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Long:
			if (!sourceProperty.isMany()) {
				long resultLong = DataConverter.INSTANCE.toLong(
					    sourceProperty.getType(), value);
			    result = Bytes.toBytes(resultLong);
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Short:
			if (!sourceProperty.isMany()) {
				short resultShort = DataConverter.INSTANCE.toShort(sourceProperty
						.getType(), value);
				result = Bytes.toBytes(resultShort);
			}
			else {				
				String strResult = DataConverter.INSTANCE.toString(sourceProperty, value);
				result = Bytes.toBytes(strResult);
			}
			break;
		case Object:
			// FIXME: do we serialize objects in some custom format for hbase
		default:
			resultStr = DataConverter.INSTANCE.toString(sourceProperty
					.getType(), value);
			result = Bytes.toBytes(resultStr);
			break;
		}
		return result;
	}

}
