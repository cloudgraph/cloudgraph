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

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Provides access to the operational, configuration and other state information
 * required for write operations on a single graph table.
 * <p>
 * Acts as a container for one or more {@link RowWriter} elements and
 * encapsulates the HBase client <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html"
 * >Put</a> and <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Delete.html"
 * >Delete</a> operations for use in write operations across one or more graph
 * rows within a table.
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.RowWriter
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface TableWriter extends TableOperation {

	public DistributedWriter getDistributedWriter();
	@Deprecated
	public void setDistributedWriter(
			DistributedGraphWriter distributedGraphWriter);
	/**
	 * Returns the row writer context for the given UUID
	 * 
	 * @param uuid
	 *            the UUID string
	 * @return the row writer context for the given UUID
	 */
	public RowWriter getRowWriter(UUID uuid);

	/**
	 * Adds the given row writer context mapping it to the given UUID.
	 * 
	 * @param uuid
	 *            the UUID
	 * @param rowContext
	 *            the row writer context
	 */
	public void addRowWriter(UUID uuid, RowWriter rowContext);

	/**
	 * Returns all row writer context values for this table context.
	 * 
	 * @return all row writer context values for this table context.
	 */
	public List<RowWriter> getAllRowWriters();

	/**
	 * Closes tables and connections.
	 */
	public void close() throws IOException;
}
