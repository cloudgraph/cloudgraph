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
package org.cloudgraph.store.service;

/**
 * Exception that indicates a row was not found but expected within a given
 * table.
 * 
 * <p>
 * Note that copied data objects have the same data properties as the source but
 * have new (and therefore different) underlying <a target="#"
 * href="http://docs.oracle.com/javase/6/docs/api/java/util/UUID.html">UUID</a>
 * and other management properties which are not defined within the source Type.
 * Use copied data objects to help automate and save save effort when creating
 * <b>NEW</b> data objects. To simply link/add and existing data object to a new
 * data graph, first use {@link DataObject.detach()} to remove it from its
 * graph. Than add it to a another graph.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class MissingRowException extends GraphServiceException {
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor.
	 * 
	 * @param table
	 *            the name of the table for the expected row
	 * @param rowKey
	 *            the row key
	 */
	public MissingRowException(String table, String rowKey) {
		super(
				"expected row for key '"
						+ rowKey
						+ "' for table '"
						+ table
						+ "' - note: if the composite row key includes a UUID "
						+ "and a copied data object was used (CopyHelper creates a new UUID), "
						+ "the row will not be found - use DataObject.detach(), then add "
						+ "the existing detached data object");
	}

}