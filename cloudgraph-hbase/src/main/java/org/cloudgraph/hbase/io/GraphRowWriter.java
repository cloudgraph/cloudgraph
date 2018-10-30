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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.hbase.key.StatefullColumnKeyFactory;
import org.cloudgraph.hbase.mutation.Mutations;
import org.cloudgraph.hbase.mutation.Qualifiers;
import org.cloudgraph.hbase.service.HBaseDataConverter;
import org.cloudgraph.state.ProtoSequenceGenerator;
import org.cloudgraph.state.SequenceGenerator;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.service.DuplicateRowException;
import org.cloudgraph.store.service.GraphServiceException;
import org.cloudgraph.store.service.MissingRowException;
import org.cloudgraph.store.service.ToumbstoneRowException;
import org.plasma.sdo.Concurrent;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.helper.DataConverter;
import org.plasma.sdo.profile.ConcurrencyType;

import commonj.sdo.ChangeSummary;
import commonj.sdo.DataObject;

/**
 * The operational, configuration and other state information required for write
 * operations on a single graph row.
 * <p>
 * Acts as a single component within a {@link TableWriter} container and
 * encapsulates the HBase client <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html"
 * >Put</a> and <a target="#" href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html"
 * >Put</a> operations for use in write operations across multiple logical
 * entities within a graph row.
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.TableWriter
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class GraphRowWriter extends DefaultRowOperation implements RowWriter {

  private static Log log = LogFactory.getLog(GraphRowWriter.class);
  private TableWriter tableWriter;

  private Put row;
  private Delete delete;
  private Increment increment;

  private Map<Integer, EdgeWriter> edgeWriterMap = new HashMap<Integer, EdgeWriter>();
  private Qualifiers qualifierMap = new Qualifiers();
  private Map<DataObject, Long> dataObjectSequenceMap = new HashMap<>();

  public GraphRowWriter(byte[] rowKey, DataObject rootDataObject, TableWriter tableWriter) {
    super(rowKey, rootDataObject);
    this.tableWriter = tableWriter;
    this.row = new Put(rowKey);
  }

  @Override
  public SequenceGenerator getSequenceMapping() throws IOException {
    if (this.sequenceMapping == null) {
      this.sequenceMapping = createSequenceMapping(this.rowKey, this.rootDataObject,
          this.rootDataObject.getDataGraph().getChangeSummary());
    }
    return this.sequenceMapping;
  }

  @Override
  public GraphStatefullColumnKeyFactory getColumnKeyFactory() throws IOException {
    if (this.columnKeyFactory == null) {
      this.columnKeyFactory = new StatefullColumnKeyFactory(this);
    }
    return this.columnKeyFactory;
  }

  // @Override
  private Put getPut() {
    return this.row;
  }

  @Override
  public void deleteRow() {
    if (this.delete == null) {
      this.delete = new Delete(this.getRowKey());
    }
  }

  /**
   * Returns the existing (or creates a new) row delete mutation.
   * 
   * @return the existing (or creates a new) row delete mutation.
   */
  // @Override
  private Delete getDelete() {
    if (this.delete == null) {
      this.delete = new Delete(this.getRowKey());
    }
    return this.delete;
  }

  /**
   * Returns whether there is an existing row delete mutation.
   * 
   * @return whether there is an existing row delete mutation.
   */
  @Override
  public boolean hasRowDelete() {
    return this.delete != null;
  }

  // @Override
  private Increment getIncrement() {
    if (this.increment == null) {
      this.increment = new Increment(this.getRowKey());
    }
    return this.increment;
  }

  /**
   * Returns a single column value for this row given a context data object and
   * property. Uses a statefull column key factory to generate a column key
   * based on the given context data object and property.
   * 
   * @param dataObject
   *          the context data object
   * @param property
   *          the context property
   * @return the column value bytes
   * @throws IOException
   * 
   * @see StatefullColumnKeyFactory
   */
  @Override
  public byte[] fetchColumnValue(PlasmaDataObject dataObject, PlasmaProperty property)
      throws IOException {
    byte[] qualifier = this.getColumnKeyFactory().createColumnKey(
        (PlasmaType) dataObject.getType(), property);

    Get existing = new Get(this.rowKey);

    byte[] family = tableWriter.getTableConfig().getDataColumnFamilyNameBytes();
    existing.addColumn(family, qualifier);

    Result result = this.getTableWriter().getTable().get(existing);
    return result.getValue(family, qualifier);
  }

  @Override
  public TableWriter getTableWriter() {
    return this.tableWriter;
  }

  /**
   * Returns whether the root data object for this writer is created.
   * 
   * @return whether the root data object for this writer is created.
   */
  @Override
  public boolean isRootCreated() {
    return this.rootDataObject.getDataGraph().getChangeSummary().isCreated(this.rootDataObject);
  }

  /**
   * Returns whether the root data object for this writer is deleted.
   * 
   * @return whether the root data object for this writer is deleted.
   */
  @Override
  public boolean isRootDeleted() {
    return this.rootDataObject.getDataGraph().getChangeSummary().isDeleted(this.rootDataObject);
  }

  @Override
  public Mutations getWriteOperations() {
    List<Row> rows = new ArrayList<>(2);
    // if any qualifiers
    if (this.row != null && this.row.size() > 0)
      rows.add(this.row);
    // for delete , can be just the oper with no qualifiers
    if (this.delete != null)
      rows.add(this.delete);

    if (this.increment != null)
      rows.add(this.increment);

    return new Mutations(rows, this.qualifierMap);
  }

  /**
   * Initializes a graph state by querying for a row based on the given row key
   * and either creating a new (empty) graph state for an entirely new graph, or
   * otherwise initializing a graph state based on state or state and management
   * columns in the existing returned row.
   * 
   * @param rowKey
   *          the row key
   * @param dataGraph
   *          the data graph
   * @param changeSummary
   *          the change summary
   * @return the graph state
   * @throws IOException
   * @throws DuplicateRowException
   *           for a new graph if a row already exists for the given row key
   * @throws GraphServiceException
   *           where except for a new graph, if no row exists for the given row
   *           key
   */
  protected SequenceGenerator createSequenceMapping(byte[] rowKey, DataObject dataObject,
      ChangeSummary changeSummary) throws IOException {
    SequenceGenerator graphState;
    // --ensure row exists unless a new row/graph
    // --use empty get with only necessary "state" management columns

    // if entirely new graph for the given
    // distributed or sub-graph root
    if (changeSummary.isCreated(dataObject)) {
      TableMapping tableConfig = this.tableWriter.getTableConfig();
      if (tableConfig.uniqueChecks()) {
        Result result = getMinimalRow(rowKey, tableConfig, this.tableWriter.getTable());
        if (!result.isEmpty()) {
          if (!result.containsColumn(tableConfig.getDataColumnFamilyNameBytes(),
              GraphMetaKey.TOMBSTONE.codeAsBytes())) {
            throw new DuplicateRowException("no row for id '" + Bytes.toString(rowKey)
                + "' expected when creating new row for table '" + tableConfig.getTable().getName()
                + "'");
          } else {
            if (!tableConfig.tombstoneRowsOverwriteable())
              throw new ToumbstoneRowException("no toumbstone row for id '"
                  + Bytes.toString(rowKey) + "' expected when creating new row for table '"
                  + tableConfig.getTable().getName() + "' - cannot overwrite toumbstone row");
          }
        }
      }
      PlasmaDataObject root = (PlasmaDataObject) dataObject;
      // graphState = new BindingSequenceGenerator(root.getUUID(),
      // this.tableWriter.getDistributedOperation().getMarshallingContext());
      graphState = new ProtoSequenceGenerator();
      if (log.isDebugEnabled())
        log.debug(graphState.toString());
    } else { // modify or delete
      TableMapping tableConfig = this.tableWriter.getTableConfig();
      Result result = getStateRow(rowKey, tableConfig, this.tableWriter.getTable());
      if (result.isEmpty()) {
        throw new MissingRowException(tableConfig.getTable().getName(), Bytes.toString(rowKey));
      }
      if (result.containsColumn(tableConfig.getDataColumnFamilyNameBytes(),
          GraphMetaKey.TOMBSTONE.codeAsBytes())) {
        throw new ToumbstoneRowException("no row for id '" + Bytes.toString(rowKey)
            + "' expected when modifying row for table '" + tableConfig.getTable().getName()
            + "' - cannot overwrite toumbstone row");
      }
      byte[] state = result.getValue(Bytes.toBytes(tableConfig.getDataColumnFamilyName()),
          GraphMetaKey.SEQUENCE_MAPPING.codeAsBytes());
      if (state != null) {
        if (log.isDebugEnabled()) {
          log.debug(" state: " + Bytes.toString(state));
        }
        graphState = new ProtoSequenceGenerator(state);
      } else
        graphState = new ProtoSequenceGenerator();

      // graphState = new BindingSequenceGenerator(Bytes.toString(state),
      // this.tableWriter.getDistributedOperation().getMarshallingContext());
      if (log.isDebugEnabled())
        log.debug(graphState.toString());

      // Even though we found a row, the user could have committed a data object
      // which was copied and has a different UUID than the original data object
      // which generated the graph state. Subsequent update or delete operations
      // using
      // this mismatched UUID can cause havoc, as the UUID is a key used to look
      // up
      // sequence values from the state and create column keys and modify or
      // delete
      // associated values.
      byte[] uuidQual = getColumnKeyFactory().createColumnKey(this.getRootType(),
          EntityMetaKey.UUID);
      byte[] rootUuid = result.getValue(Bytes.toBytes(tableConfig.getDataColumnFamilyName()),
          uuidQual);
      if (rootUuid == null)
        throw new OperationException("expected column '" + EntityMetaKey.UUID + " for row "
            + Bytes.toString(rowKey) + "'");
      UUID uuid = UUID.fromString(new String(rootUuid, tableConfig.getCharset()));

      PlasmaDataObject root = (PlasmaDataObject) dataObject;
      if (!uuid.equals(root.getUUID()))
        throw new UUIDMismatchException(
            "Graph state root UUID '"
                + uuid
                + "' "
                + "does not match writer sub-root, "
                + root
                + " - can be caused by data object copy operations, "
                + "where only properties are copied not the UUID, then the copied object is modified and comitted");
    }
    return graphState;
  }

  private Result getMinimalRow(byte[] rowKey, TableMapping tableConfig, Table table)
      throws IOException {
    Get existing = new Get(rowKey);
    byte[] fam = tableConfig.getDataColumnFamilyNameBytes();
    byte[] uuidQual = getColumnKeyFactory().createColumnKey(this.getRootType(), EntityMetaKey.UUID);
    existing.addColumn(fam, uuidQual);
    byte[] rootTypeQual = getColumnKeyFactory().createColumnKey(this.getRootType(),
        EntityMetaKey.UUID);
    existing.addColumn(fam, rootTypeQual);
    existing.addColumn(fam, GraphMetaKey.TOMBSTONE.codeAsBytes());
    return table.get(existing);
  }

  private Result getStateRow(byte[] rowKey, TableMapping tableConfig, Table table)
      throws IOException {
    Get existing = new Get(rowKey);
    byte[] fam = tableConfig.getDataColumnFamilyNameBytes();
    byte[] uuidQual = getColumnKeyFactory().createColumnKey(this.getRootType(), EntityMetaKey.UUID);
    existing.addColumn(fam, uuidQual);
    byte[] rootTypeQual = getColumnKeyFactory().createColumnKey(this.getRootType(),
        EntityMetaKey.UUID);
    existing.addColumn(fam, rootTypeQual);
    for (GraphMetaKey field : GraphMetaKey.values()) {
      existing.addColumn(fam, field.codeAsBytes());
    }
    return table.get(existing);
  }

  @Override
  public EdgeWriter getEdgeWriter(PlasmaDataObject dataObject, PlasmaProperty property,
      long sequence) throws IOException {
    int hashCode = getHashCode(dataObject, property);
    EdgeWriter edgeWriter = edgeWriterMap.get(hashCode);
    if (edgeWriter == null) {
      if (sequence > 0) {
        edgeWriter = new GraphEdgeWriter(dataObject, property, sequence, this.getTableWriter()
            .getTableConfig(), this.graphConfig, this);
      } else { // its the root data object
        edgeWriter = new GraphEdgeWriter(dataObject, property, this.getTableWriter()
            .getTableConfig(), this.graphConfig, this);
      }
      edgeWriterMap.put(hashCode, edgeWriter);
    }
    return edgeWriter;
  }

  @Override
  public long newSequence(PlasmaDataObject dataObject) throws IOException {
    Long sequence = getSequenceMapping().nextSequence(dataObject);
    return sequence;
  }

  @Override
  public void writeRowEntityMetaData(PlasmaDataObject dataObject, long sequence) throws IOException {
    byte[] fam = this.getTableWriter().getTableConfig().getDataColumnFamilyNameBytes();

    PlasmaType plasmaType = (PlasmaType) dataObject.getType();
    byte[] uuidQual = null;
    byte[] typeQual = null;
    if (sequence > 0) {
      uuidQual = this.getColumnKeyFactory().createColumnKey(plasmaType, sequence,
          EntityMetaKey.UUID);
      typeQual = this.getColumnKeyFactory().createColumnKey(plasmaType, sequence,
          EntityMetaKey.TYPE);
    } else {
      uuidQual = this.getColumnKeyFactory().createColumnKey(plasmaType, EntityMetaKey.UUID);
      typeQual = this.getColumnKeyFactory().createColumnKey(plasmaType, EntityMetaKey.TYPE);
    }
    this.getPut().addColumn(fam, uuidQual, Bytes.toBytes(dataObject.getUUIDAsString()));
    this.getPut().addColumn(fam, typeQual, encodeType(plasmaType));
  }

  @Override
  public void deleteRowEntityMetaData(PlasmaDataObject dataObject, long sequence)
      throws IOException {
    byte[] fam = this.getTableWriter().getTableConfig().getDataColumnFamilyNameBytes();
    GraphStatefullColumnKeyFactory keyFac = this.getColumnKeyFactory();
    PlasmaType plasmaType = (PlasmaType) dataObject.getType();
    byte[] qual = null;
    for (EntityMetaKey metaField : EntityMetaKey.values()) {
      if (sequence > 0)
        qual = keyFac.createColumnKey(plasmaType, sequence, metaField);
      else
        qual = keyFac.createColumnKey(plasmaType, metaField);
      this.getDelete().addColumns(fam, qual); // deletes all cell versions
    }
  }

  @Override
  public void writeRowData(PlasmaDataObject dataObject, long sequence, PlasmaProperty property,
      byte[] value) throws IOException {
    byte[] fam = this.getTableWriter().getTableConfig().getDataColumnFamilyNameBytes();
    PlasmaType plasmaType = (PlasmaType) dataObject.getType();
    GraphStatefullColumnKeyFactory keyFac = this.getColumnKeyFactory();

    byte[] qual = null;
    if (sequence > 0)
      qual = keyFac.createColumnKey(plasmaType, sequence, property);
    else
      qual = keyFac.createColumnKey(plasmaType, property);
    this.getPut().addColumn(fam, qual, value);
    this.qualifierMap.add(fam, qual, dataObject, property);

    // For concurrent types, annotate the mutation with bytes data
    // from the concurrency property
    if (plasmaType.isConcurrent() && !dataObject.getDataGraph().getChangeSummary().isCreated(dataObject)) {
      // If we haven't already done this
      if ((this.readRowAttribute(RowWriter.ROW_ATTR_NAME_IS_CONCURRENT_BOOL)) == null) {
        this.writeRowAttribute(RowWriter.ROW_ATTR_NAME_IS_CONCURRENT_BOOL,
            Bytes.toBytes(Boolean.TRUE));
        PlasmaProperty concurrProp = (PlasmaProperty) plasmaType
            .findProperty(ConcurrencyType.optimistic);
        Concurrent concurrent = concurrProp.getConcurrent();
        Object dataValue = dataObject.get(concurrProp);
        if (dataValue == null)
          throw new GraphServiceException("expected data value for concurrent property, "
              + concurrProp + ", with datatype " + concurrProp.getType() + "");
        switch (concurrent.getDataFlavor()) {
        case version:
          // always bump version by 1 for update on managed concurrent version
          // prop
          long longDataValue = DataConverter.INSTANCE.toLong(property.getType(), dataValue);
          byte[] valueBytes = HBaseDataConverter.INSTANCE.toBytes(property, longDataValue);
          byte[] concurrQual = null;
          if (sequence > 0)
            concurrQual = keyFac.createColumnKey(plasmaType, sequence, concurrProp);
          else
            concurrQual = keyFac.createColumnKey(plasmaType, concurrProp);
  
          this.writeRowAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_FAM_BYTES, fam);
          this.writeRowAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_QUAL_BYTES, concurrQual);
          this.writeRowAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_VALUE_BYTES, valueBytes);
  
          this.getTableWriter().setHasConcurrentRows(true);
          
          // increment and set this managed concurrent property
          longDataValue++;
          byte[] updatedValueBytes = HBaseDataConverter.INSTANCE.toBytes(property, longDataValue);
          this.getPut().addColumn(fam, concurrQual, updatedValueBytes);
          
          break;
        default:
          throw new GraphServiceException("unsupported concurrent data flavor ("
              + concurrent.getDataFlavor() + ") for property, " + concurrProp + ", with datatype "
              + concurrProp.getType() + "");
        }
      }
    }

    if (log.isDebugEnabled())
      log.debug("writing " + Bytes.toString(qual) + " / " + Bytes.toString(value));
  }

  @Override
  public void writeRowData(byte[] fam, byte[] qualifier, byte[] value) throws IOException {
    this.getPut().addColumn(fam, qualifier, value);
  }

  @Override
  public void writeRowAttribute(String name, byte[] value) throws IOException {
    this.getPut().setAttribute(name, value);
  }

  @Override
  public byte[] readRowAttribute(String name) throws IOException {
    return this.getPut().getAttribute(name);
  }

  @Override
  public void deleteRowData(PlasmaDataObject dataObject, long sequence, PlasmaProperty property)
      throws IOException {
    byte[] fam = this.getTableWriter().getTableConfig().getDataColumnFamilyNameBytes();
    PlasmaType plasmaType = (PlasmaType) dataObject.getType();
    GraphStatefullColumnKeyFactory keyFac = this.getColumnKeyFactory();

    byte[] qual = null;
    if (sequence > 0)
      qual = keyFac.createColumnKey(plasmaType, sequence, property);
    else
      qual = keyFac.createColumnKey(plasmaType, property);
    this.getDelete().addColumns(fam, qual); // deletes all cell versions
    this.qualifierMap.add(fam, qual, dataObject, property);

    if (log.isDebugEnabled())
      log.debug("deleting " + Bytes.toString(qual));
  }

  @Override
  public void deleteRowData(byte[] fam, byte[] qualifier) throws IOException {
    getDelete().addColumns(fam, qualifier); // deletes all cell version
  }

  @Override
  public void incrementRowData(PlasmaDataObject dataObject, long sequence, PlasmaProperty property,
      long value) throws IOException {
    byte[] fam = this.getTableWriter().getTableConfig().getDataColumnFamilyNameBytes();
    PlasmaType plasmaType = (PlasmaType) dataObject.getType();
    GraphStatefullColumnKeyFactory keyFac = this.getColumnKeyFactory();

    byte[] qual = null;
    if (sequence > 0)
      qual = keyFac.createColumnKey(plasmaType, sequence, property);
    else
      qual = keyFac.createColumnKey(plasmaType, property);

    this.getIncrement().addColumn(fam, qual, value);
    this.qualifierMap.add(fam, qual, dataObject, property);

    if (log.isDebugEnabled())
      log.debug("incrementing " + Bytes.toString(qual) + " / " + value);
  }

  @Override
  public void addSequence(DataObject dataObject, long sequence) {
    Long existing = this.dataObjectSequenceMap.get(dataObject);
    if (existing != null)
      throw new IllegalArgumentException("sequence (" + existing
          + ") already mapped for data object, " + dataObject);
    this.dataObjectSequenceMap.put(dataObject, sequence);
  }

  @Override
  public boolean containsSequence(DataObject dataObject) {
    return this.dataObjectSequenceMap.containsKey(dataObject);
  }

  @Override
  public long getSequence(DataObject dataObject) {
    Long result = this.dataObjectSequenceMap.get(dataObject);
    if (result == null)
      throw new IllegalArgumentException("sequence not found for data object, " + dataObject);
    return result;
  }

  @Override
  public byte[] encodeRootType() {
    return encodeType(this.getRootType());
  }

  @Override
  public byte[] encodeType(PlasmaType type) {
    return encode(type);
  }

  public static byte[] encode(PlasmaType type) {
    byte[] uriPhysicalName = type.getURIPhysicalNameBytes();
    byte[] uri = type.getURIBytes();
    byte[] physicalName = type.getPhysicalNameBytes();
    byte[] name = type.getNameBytes();
    if (uriPhysicalName != null && physicalName != null) {
      return org.cloudgraph.common.Bytes.concat(uriPhysicalName, Bytes.toBytes(ROOT_TYPE_DELIM),
          physicalName);
    } else {
      log.warn("no qualified physical name available for type, "
          + type
          + ", encoding qualified logical name - please annotate your model with physical name aliases to facilitate logical/physical name isolation");
      return org.cloudgraph.common.Bytes.concat(uri, Bytes.toBytes(ROOT_TYPE_DELIM), name);
    }
  }

}
