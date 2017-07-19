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


/**
 * Represents the literal for an individual field within a composite partial 
 * row key scan and provides access to start and stop byte sequences for 
 * various relational operators under the HBase partial row-key 
 * scan for various optionally configurable hashing, 
 * formatting, padding and other features.
 * @author Scott Cinnamond
 * @since 0.5.3
 * @see PartialRowKey
 * @see FuzzyRowKeyLiteral
 */
public interface PartialRowKeyLiteral {

	/**
	 * Returns the HBase scan "start-row" composite row-key field 
	 * bytes for this literal under the various relational, logical 
	 * operator and other optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the HBase scan "start-row" composite row-key field 
	 * bytes for this literal under the various relational, logical 
	 * operator and other optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getStartBytes();
	
	/**
	 * Returns the HBase scan "stop-row" composite row-key field 
	 * bytes for this literal under the various relational, logical 
	 * operator and other optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the HBase scan "stop-row" composite row-key field 
	 * bytes for this literal under the various relational, logical 
	 * operator and other optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getStopBytes(); 

    /**
	 * Returns the "start row" bytes 
	 * used to represent "equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getEqualsStartBytes();
	
	/**
	 * Returns the "stop row" bytes 
	 * used to represent "equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getEqualsStopBytes();
	
	/**
	 * Returns the "start row" bytes 
	 * used to represent "greater than" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "greater than" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getGreaterThanStartBytes();

	/**
	 * Returns the "stop row" bytes 
	 * used to represent "greater than" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "greater than" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getGreaterThanStopBytes();

	/**
	 * Returns the "start row" bytes 
	 * used to represent "greater than equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "greater than equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getGreaterThanEqualStartBytes();

	/**
	 * Returns the "stop row" bytes 
	 * used to represent "greater than equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "greater than equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */
	public byte[] getGreaterThanEqualStopBytes();
	
	/**
	 * Returns the "start row" bytes 
	 * used to represent "less than" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "less than" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */	
	public byte[] getLessThanStartBytes();
	
	/**
	 * Returns the "stop row" bytes 
	 * used to represent "less than" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "less than" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */	
	public byte[] getLessThanStopBytes();
	
	/**
	 * Returns the "start row" bytes 
	 * used to represent "less than equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "start row" bytes 
	 * used to represent "less than equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */	
	public byte[] getLessThanEqualStartBytes();

	/**
	 * Returns the "stop row" bytes 
	 * used to represent "less than equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 * @return the "stop row" bytes 
	 * used to represent "less than equals" relational operator 
	 * under an HBase partial row-key scan under 
	 * the various optionally configurable hashing, 
	 * formatting and padding features.
	 */	
	public byte[] getLessThanEqualStopBytes();
	
}
