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
package org.cloudgraph.hbase.scan;

import java.math.BigDecimal;
import java.util.Arrays;

import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.plasma.query.model.RelationalOperator;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.DataType;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Type;

/**
 * A real data "flavor" specific literal class used to abstract 
 * the complexities involved in assembling the various 
 * segments and fields of composite (scan start/stop) row keys 
 * under various relational and logical operator and
 * various configurable composite key-field hashing, formatting, padding 
 * and other features. A real literal does not contain or involve wildcards but
 * nevertheless may "participate" in a fuzzy scan as part of a composite row key and
 * therefore implements {@link FuzzyRowKeyLiteral} supplying only default key and
 * info bytes.  
 * 
 * 
 * @see org.cloudgraph.config.TableConfig
 * @see org.cloudgraph.hbase.service.HBaseDataConverter
 * @author Scott Cinnamond
 * @since 0.5
 */
public class RealLiteral extends ScanLiteral 
    implements PartialRowKeyLiteral, FuzzyRowKeyLiteral, CompleteRowKeyLiteral {

	public static final float INCREMENT_FLOAT = Float.MIN_VALUE;
	public static final double INCREMENT_DOUBLE = Double.MIN_VALUE;
	public static final BigDecimal INCREMENT_DECIMAL = BigDecimal.valueOf(Double.MIN_VALUE);

	public RealLiteral(String literal,
			PlasmaType rootType,
			RelationalOperator relationalOperator,
			UserDefinedRowKeyFieldConfig fieldConfig) {
		super(literal, rootType, relationalOperator, 
			  fieldConfig);
	}
	
	/**
	 * Returns the "start row" bytes 
	 * used to represent "equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getEqualsStartBytes() {
		byte[] startBytes = null;
		Object startValue = this.dataConverter.convert(property.getType(), this.literal);
		String startValueStr = this.dataConverter.toString(property.getType(), startValue);
		if (this.fieldConfig.isHash()) {
			startBytes = this.hashing.toStringBytes(startValueStr);
			startBytes = this.padding.pad(startBytes, 
					this.fieldConfig.getMaxLength(), 
					DataFlavor.integral);
		}
		else {
			startBytes = startValueStr.getBytes(this.charset);
			startBytes = this.padding.pad(startBytes, 
					this.fieldConfig.getMaxLength(), 
					this.fieldConfig.getDataFlavor());
		}
		return startBytes;
	}
	
	/**
	 * Returns the "stop row" bytes 
	 * used to represent "equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getEqualsStopBytes() {
		byte[] stopBytes = null;
		Object value = this.dataConverter.convert(property.getType(), this.literal);
		if (this.fieldConfig.isHash()) {
			String stopValueStr = incrementHash(property.getType(), value);
			stopBytes = stopValueStr.getBytes(this.charset);
			stopBytes = this.padding.pad(stopBytes, 
					this.fieldConfig.getMaxLength(), 
					DataFlavor.integral);
		}
		else {
			String stopValueStr = increment(property.getType(), value);
			stopBytes = stopValueStr.getBytes(this.charset);
			stopBytes = this.padding.pad(stopBytes, 
					this.fieldConfig.getMaxLength(), 
					this.fieldConfig.getDataFlavor());
		}
		return stopBytes;
	}	
	
	/**
	 * Returns the "start row" bytes 
	 * used to represent "greater than" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "greater than" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getGreaterThanStartBytes() {
		byte[] startBytes = null;
		Object value = this.dataConverter.convert(property.getType(), this.literal);
		if (this.fieldConfig.isHash()) {
			String startValueStr = incrementHash(property.getType(), value);
			startBytes = startValueStr.getBytes(this.charset);
			startBytes = this.padding.pad(startBytes, 
					this.fieldConfig.getMaxLength(), 
					DataFlavor.integral);
		}
		else {
			String startValueStr = increment(property.getType(), value);
			startBytes = startValueStr.getBytes(this.charset);
			startBytes = this.padding.pad(startBytes, 
					this.fieldConfig.getMaxLength(), 
					this.fieldConfig.getDataFlavor());
		}
		return startBytes;
	}
	
	/**
	 * Returns the "stop row" bytes 
	 * used to represent "greater than" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "greater than" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getGreaterThanStopBytes() {
	    return new byte[0];
	}
	
	/**
	 * Returns the "start row" bytes 
	 * used to represent "greater than equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "greater than equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getGreaterThanEqualStartBytes() {
		byte[] startBytes = null;
		Object startValue = this.dataConverter.convert(property.getType(), this.literal);
		String startValueStr = this.dataConverter.toString(property.getType(), startValue);
		if (fieldConfig.isHash()) {
			startBytes = this.hashing.toStringBytes(startValueStr);
			startBytes = this.padding.pad(startBytes, 
					this.fieldConfig.getMaxLength(), 
					DataFlavor.integral);
		}
		else {
			startBytes = startValueStr.getBytes(this.charset);
			startBytes = this.padding.pad(startBytes, 
					this.fieldConfig.getMaxLength(), 
					this.fieldConfig.getDataFlavor());
		}
		return startBytes;
	}
	
	/**
	 * Returns the "stop row" bytes 
	 * used to represent "greater than equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "greater than equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getGreaterThanEqualStopBytes() {
	    return new byte[0];
	}
	
	/**
	 * Returns the "start row" bytes 
	 * used to represent "less than" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "less than" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */	
	public byte[] getLessThanStartBytes() {
	    return new byte[0];
	}
	
	/**
	 * Returns the "stop row" bytes 
	 * used to represent "less than" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "less than" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */	
	public byte[] getLessThanStopBytes() {
		byte[] stopBytes = null;
		Object stopValue = this.dataConverter.convert(property.getType(), this.literal);
		// Note: in HBase the stop row is exclusive, so just use
		// the literal value, no need to decrement it
		String stopValueStr = this.dataConverter.toString(property.getType(), stopValue);
		if (fieldConfig.isHash()) {
			stopBytes = this.hashing.toStringBytes(stopValueStr);
			stopBytes = this.padding.pad(stopBytes, 
					this.fieldConfig.getMaxLength(), 
					DataFlavor.integral);
		}
		else {
			stopBytes = stopValueStr.getBytes(this.charset);
			stopBytes = this.padding.pad(stopBytes, 
					this.fieldConfig.getMaxLength(), 
					this.fieldConfig.getDataFlavor());
		}
		return stopBytes;
	}
	
	/**
	 * Returns the "start row" bytes 
	 * used to represent "less than equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "less than equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */	
	public byte[] getLessThanEqualStartBytes() {
	    return new byte[0];
	}
	
	/**
	 * Returns the "stop row" bytes 
	 * used to represent "less than equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "less than equals" relational operator 
	 * under an HBase partial row-key scan for this real (data flavor) literal under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */	
	public byte[] getLessThanEqualStopBytes() {
		byte[] stopBytes = null;
		Object value = this.dataConverter.convert(property.getType(), this.literal);
		if (this.fieldConfig.isHash()) {
			String stopValueStr = incrementHash(property.getType(), value);
			stopBytes = stopValueStr.getBytes(this.charset);
			stopBytes = this.padding.pad(stopBytes, 
					this.fieldConfig.getMaxLength(), 
					DataFlavor.integral);
		}
		else {
			String stopValueStr = increment(property.getType(), value);
			stopBytes = stopValueStr.getBytes(this.charset);
			stopBytes = this.padding.pad(stopBytes, 
					this.fieldConfig.getMaxLength(), 
					this.fieldConfig.getDataFlavor());
		}
		return stopBytes;
	}
	
	/**
	 * Returns the bytes 
	 * used to represent an "equals" relational operator 
	 * for a specific composite row key field, under an HBase 'Get' operation for 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the bytes 
	 * used to represent an "equals" relational operator 
	 * for a specific composite row key field, under an HBase 'Get' operation for 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	@Override
	public byte[] getEqualsBytes() {
		return getEqualsStartBytes();
	}
	
	@Override
	public byte[] getFuzzyKeyBytes() {
		return getEqualsStartBytes();
	}

	@Override
	public byte[] getFuzzyInfoBytes() {
		byte[] infoBytes = new byte[this.fieldConfig.getMaxLength()];
		Arrays.fill(infoBytes, (byte)0); // fixed char 
		return infoBytes;
	}
	
	private String incrementHash(Type type, Object value) {
		String valueStr = this.dataConverter.toString(property.getType(), value);
		String result = this.hashing.toString(valueStr, HASH_INCREMENT);
		return result;
	}
	
	private String increment(Type type, Object value) {
		String result = "";
        DataType sourceDataType = DataType.valueOf(type.getName());
        switch (sourceDataType) {
        case Float:
    		Float floatValue = this.dataConverter.toFloat(property.getType(), value);
		    int intBits = Float.floatToRawIntBits(floatValue.floatValue());
		    intBits++;
		    Float floatResult = Float.valueOf(Float.intBitsToFloat(intBits));
		    result = this.dataConverter.toString(type, floatResult);
    		break;
        case Double:
    		Double doubleValue = this.dataConverter.toDouble(property.getType(), value);
		    long longBits = Double.doubleToRawLongBits(doubleValue.doubleValue());
		    longBits++;
		    Double doubleResult = Double.valueOf(Double.longBitsToDouble(longBits));
		    result = this.dataConverter.toString(type, doubleResult);
    		break;
        case Decimal:        	        	
    		BigDecimal decimalValue = this.dataConverter.toDecimal(property.getType(), value);
    		//FIXME: loss of precision
    		double temp = decimalValue.doubleValue();
		    longBits = Double.doubleToRawLongBits(temp);
		    longBits++;
		    doubleResult = Double.valueOf(Double.longBitsToDouble(longBits));
		    result = this.dataConverter.toString(type, doubleResult);
    		break;
        default:
        	throw new ScanException("expected real (Float, Double, Decinal)datatype not, "
        			+ sourceDataType.name());
        }
        return result;
	}



}
