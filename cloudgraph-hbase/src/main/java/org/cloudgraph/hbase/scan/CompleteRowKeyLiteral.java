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
 * Represents the literal for an individual field within a composite  
 * row key and provides access to the key byte sequence for 
 * various supported relational operators under a row-key 
 * 'Get' operation for various optionally configurable hashing, 
 * formatting, padding and other features.
 * @author Scott Cinnamond
 * @since 0.5.5
 * @see CompleteRowKeyScan
 * @see CompleteRowKeyLiteral
 */
public interface CompleteRowKeyLiteral {

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
	public byte[] getEqualsBytes();
	
}
