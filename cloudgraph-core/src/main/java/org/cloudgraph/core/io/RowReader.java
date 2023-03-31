/**
 * Copyright 2017 TerraMeta Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cloudgraph.core.io;

import java.io.IOException;

import org.cloudgraph.core.client.CellValues;
import org.cloudgraph.core.io.EdgeReader;
import org.cloudgraph.core.io.RowOperation;
import org.cloudgraph.core.io.TableReader;
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
  public CellValues getRow();

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
