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
package org.cloudgraph.hbase.io;

/**
 * For where 2 UUID's or UUID strings from 2 different contexts are expected
 * to match. 
 * <p></p>
 * Thrown where e.g. a UUID which is queried from a data store is expected to
 * match a UUID within a data object or change summary passed to a service.   
 * @author Scott Cinnamond
 * @since 0.6.4
 */
public class UUIDMismatchException extends OperationException {

	private static final long serialVersionUID = 1L;

	public UUIDMismatchException() {
		super();
	}

	public UUIDMismatchException(String msg) {
		super(msg);
	}

	public UUIDMismatchException(Throwable t) {
		super(t);
	}
}
