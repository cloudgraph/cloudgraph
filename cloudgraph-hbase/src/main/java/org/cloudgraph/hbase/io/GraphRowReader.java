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
package org.cloudgraph.hbase.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.key.StatefullColumnKeyFactory;
import org.cloudgraph.state.ProtoSequenceGenerator;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.service.ToumbstoneRowException;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.helper.PlasmaTypeHelper;

import commonj.sdo.DataObject;

/**
 * The operational, configuration and other state information required for read
 * operations on a single graph row.
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
public class GraphRowReader extends DefaultRowOperation implements RowReader {

  private static Log log = LogFactory.getLog(GraphRowReader.class);

  private CellValues row;
  private TableReader tableReader;
  private Map<Integer, EdgeReader> edgeReaderMap = new HashMap<Integer, EdgeReader>();

  public GraphRowReader(byte[] rowKey, CellValues result, DataObject rootDataObject,
      TableReader tableReader, StoreMappingContext mappingContext) {
    super(rowKey, rootDataObject, mappingContext);
    this.row = result;
    this.tableReader = tableReader;
    byte[] state = this.row.getColumnValue(this.tableReader.getTableConfig()
        .getDataColumnFamilyNameBytes(), GraphMetaKey.SEQUENCE_MAPPING.codeAsBytes());
    if (state != null) {
      if (log.isDebugEnabled()) {
        String uuid = ((PlasmaDataObject) rootDataObject).getUUIDAsString();
        log.debug("root: " + uuid + " state: " + new String(state));
      }
    }
    byte[] toumbstone = this.row.getColumnValue(this.tableReader.getTableConfig()
        .getDataColumnFamilyNameBytes(), GraphMetaKey.TOMBSTONE.codeAsBytes());
    if (toumbstone != null)
      throw new ToumbstoneRowException("cannot read toumbstone row for root, "
          + rootDataObject.toString());

    // this.sequenceMapping = new
    // BindingSequenceGenerator(Bytes.toString(state),
    // this.tableReader.getDistributedOperation().getMarshallingContext());
    if (state != null)
      this.sequenceMapping = new ProtoSequenceGenerator(state);
    else
      this.sequenceMapping = new ProtoSequenceGenerator();
    if (log.isDebugEnabled())
      log.debug(this.sequenceMapping.toString());

    this.columnKeyFactory = new StatefullColumnKeyFactory(this);
  }

  @Override
  public CellValues getRow() {
    return this.row;
  }

  @Override
  public TableReader getTableReader() {
    return this.tableReader;
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return this.tableReader.getMappingContext();
  }

  /**
   * Frees resources associated with this reader.
   */
  public void clear() {

  }

  @Override
  public EdgeReader getEdgeReader(PlasmaType type, PlasmaProperty property, long sequence)
      throws IOException {
    int hashCode = getHashCode(type, property, sequence);
    EdgeReader edgeReader = edgeReaderMap.get(hashCode);
    if (edgeReader == null) {
      if (sequence > 0) {
        edgeReader = new GraphEdgeReader(type, property, sequence, this.getTableReader()
            .getTableConfig(), this.graphConfig, this);
      } else {
        edgeReader = new GraphEdgeReader(type, property, this.getTableReader().getTableConfig(),
            this.graphConfig, this);
      }
      edgeReaderMap.put(hashCode, edgeReader);
    }
    return edgeReader;
  }

  @Override
  public boolean edgeExists(PlasmaType type, PlasmaProperty property, long sequence)
      throws IOException {
    if (sequence > 0)
      return GraphEdgeReader.exists(type, property, sequence, this.getTableReader()
          .getTableConfig(), this.graphConfig, this);
    else
      return GraphEdgeReader.exists(type, property, this.getTableReader().getTableConfig(),
          this.graphConfig, this);
  }

  @Override
  public PlasmaType decodeType(byte[] bytes) {
    String[] tokens = Bytes.toString(bytes).split(ROOT_TYPE_DELIM);
    return (PlasmaType) PlasmaTypeHelper.INSTANCE.findTypeByPhysicalName(tokens[0], tokens[1]);
  }

}
