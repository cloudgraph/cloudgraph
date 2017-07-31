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

import org.cloudgraph.hbase.service.ColumnMap;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

/**
 * Provides access to the operational, configuration and other state information
 * required for read operations on a single graph row.
 * <p>
 * Acts as a single component within a {@link TableReader} container and
 * encapsulates the HBase client <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Get.html"
 * >Get</a> operation for use in read operations across multiple logical
 * entities within a graph row.
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.TableReader
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public interface RowReader extends RowOperation {
  public ColumnMap getRow();

  public TableReader getTableReader();

  public EdgeReader getEdgeReader(PlasmaType type, PlasmaProperty property, long sequence)
      throws IOException;

  public boolean edgeExists(PlasmaType type, PlasmaProperty property, long sequence)
      throws IOException;

  // public PlasmaType decodeRootType(byte[] bytes);

  /**
   * Frees resources associated with this.
   */
  public void clear();

  public PlasmaType decodeType(byte[] typeBytes);

}
