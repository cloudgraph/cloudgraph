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
package org.cloudgraph.rocksdb.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.Increment;
//import org.apache.hadoop.hbase.client.Mutation;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Row;
//import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.Bytes;
import org.cloudgraph.rocksdb.ext.Del;
import org.cloudgraph.rocksdb.ext.Get;
import org.cloudgraph.rocksdb.ext.Increment;
import org.cloudgraph.rocksdb.ext.Put;
import org.cloudgraph.rocksdb.ext.Result;
import org.cloudgraph.rocksdb.ext.Row;
import org.cloudgraph.rocksdb.ext.Table;
import org.cloudgraph.rocksdb.filter.ColumnInfo;
import org.cloudgraph.rocksdb.filter.Filter;
import org.cloudgraph.rocksdb.filter.MultiColumnCompareFilter;
import org.cloudgraph.rocksdb.filter.MultiColumnPrefixFilter;
import org.cloudgraph.rocksdb.key.StatefullColumnKeyFactory;
import org.cloudgraph.rocksdb.mutation.Mutation;
import org.cloudgraph.rocksdb.mutation.Mutations;
import org.cloudgraph.rocksdb.mutation.Qualifiers;
import org.cloudgraph.rocksdb.service.RocksDBDataConverter;
import org.cloudgraph.state.ProtoSequenceGenerator;
import org.cloudgraph.state.SequenceGenerator;
import org.cloudgraph.store.key.EdgeMetaKey;
import org.cloudgraph.store.key.EntityMetaKey;
import org.cloudgraph.store.key.GraphMetaKey;
import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.service.DuplicateRowException;
import org.cloudgraph.store.service.GraphServiceException;
import org.cloudgraph.store.service.MissingRowException;
import org.cloudgraph.store.service.ToumbstoneRowException;
import org.plasma.sdo.Concurrent;
import org.plasma.sdo.DataType;
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
 * @see org.cloudgraph.rocksdb.io.TableWriter
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class GraphRowWriter extends DefaultRowOperation implements RowWriter {

  private static Log log = LogFactory.getLog(GraphRowWriter.class);
  private TableWriter tableWriter;

  private Put row;
  private Del delete;
  private Increment increment;

  private Map<Integer, EdgeWriter> edgeWriterMap = new HashMap<Integer, EdgeWriter>();
  private Qualifiers qualifierMap = new Qualifiers();
  private Map<DataObject, Long> dataObjectSequenceMap = new HashMap<>();

  public GraphRowWriter(byte[] rowKey, DataObject rootDataObject, TableWriter tableWriter,
      StoreMappingContext mappingContext) {
    super(rowKey, rootDataObject, mappingContext);
    this.tableWriter = tableWriter;
    this.row = new Put(rowKey);
  }

  @Override
  public StoreMappingContext getMappingContext() {
    return this.tableWriter.getMappingContext();
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
      this.delete = new Del(rowKey);
    }
  }

  /**
   * Returns the existing (or creates a new) row delete mutation.
   * 
   * @return the existing (or creates a new) row delete mutation.
   */
  // @Override
  private Del getDelete() {
    if (this.delete == null) {
      this.delete = new Del(rowKey);
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
      this.increment = new Increment(rowKey);
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
    Map<String, ColumnInfo> map = new HashMap<>();
    byte[] family = tableWriter.getTableConfig().getDataColumnFamilyNameBytes();
    DataType propertyDataType = DataType.valueOf(property.getType().getName());
    // existing.addColumn(family, qualifier, propertyDataType);
    ColumnInfo ci = new ColumnInfo(family, qualifier, propertyDataType);
    map.put(ci.getColumn(), ci);

    MultiColumnCompareFilter filter = new MultiColumnCompareFilter(map);
    existing.setColumnFilter(filter);

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
        Result result = getMinimalRow(rowKey, tableConfig, this.getTableWriter().getTable());
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
      Result result = getStateRow(rowKey, tableConfig, this.getTableWriter().getTable());
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
    Map<String, ColumnInfo> map = new HashMap<>();

    byte[] uuidQual = getColumnKeyFactory().createColumnKey(this.getRootType(), EntityMetaKey.UUID);
    ColumnInfo ci = new ColumnInfo(fam, uuidQual, EntityMetaKey.UUID.getStorageType());
    map.put(ci.getColumn(), ci);
    // existing.addColumn(fam, ci.getColumnBytes(), ci.getDataType());

    byte[] rootTypeQual = getColumnKeyFactory().createColumnKey(this.getRootType(),
        EntityMetaKey.TYPE);
    ci = new ColumnInfo(fam, rootTypeQual, EntityMetaKey.TYPE.getStorageType());
    map.put(ci.getColumn(), ci);
    // existing.addColumn(fam, ci.getColumnBytes(), ci.getDataType());

    ci = new ColumnInfo(fam, GraphMetaKey.TOMBSTONE.codeAsBytes(),
        GraphMetaKey.TOMBSTONE.getStorageType());
    map.put(ci.getColumn(), ci);
    // existing.addColumn(fam, ci.getColumnBytes(), ci.getDataType());

    existing.setColumnFilter(new MultiColumnCompareFilter(map));

    return table.get(existing);
  }

  private Result getStateRow(byte[] rowKey, TableMapping tableConfig, Table table)
      throws IOException {
    Get existing = new Get(rowKey);
    byte[] fam = tableConfig.getDataColumnFamilyNameBytes();
    Map<String, ColumnInfo> map = new HashMap<>();

    byte[] uuidQual = getColumnKeyFactory().createColumnKey(this.getRootType(), EntityMetaKey.UUID);
    ColumnInfo ci = new ColumnInfo(fam, uuidQual, EntityMetaKey.UUID.getStorageType());
    map.put(ci.getColumn(), ci);
    // existing.addColumn(fam, ci.getColumnBytes(), ci.getDataType());

    byte[] rootTypeQual = getColumnKeyFactory().createColumnKey(this.getRootType(),
        EntityMetaKey.TYPE);
    ci = new ColumnInfo(fam, rootTypeQual, EntityMetaKey.TYPE.getStorageType());
    map.put(ci.getColumn(), ci);
    // existing.addColumn(fam, ci.getColumnBytes(), ci.getDataType());

    for (GraphMetaKey field : GraphMetaKey.values()) {
      ci = new ColumnInfo(fam, field.codeAsBytes(), field.getStorageType());
      map.put(ci.getColumn(), ci);
      // existing.addColumn(fam, field.codeAsBytes(), field.getStorageType());
    }
    MultiColumnCompareFilter filter = new MultiColumnCompareFilter(map);
    existing.setColumnFilter(filter);

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
    this.getPut().addColumn(fam, uuidQual, EntityMetaKey.UUID.getStorageType(),
        Bytes.toBytes(dataObject.getUUIDAsString()));
    this.getPut().addColumn(fam, typeQual, EntityMetaKey.TYPE.getStorageType(),
        encodeType(plasmaType));
  }

  @Override
  public void deleteRowEntityMetaData(PlasmaDataObject dataObject, long sequence)
      throws IOException {
    byte[] fam = this.getTableWriter().getTableConfig().getDataColumnFamilyNameBytes();
    Map<String, ColumnInfo> map = new HashMap<>();
    GraphStatefullColumnKeyFactory keyFac = this.getColumnKeyFactory();
    PlasmaType plasmaType = (PlasmaType) dataObject.getType();
    byte[] qual = null;
    for (EntityMetaKey metaField : EntityMetaKey.values()) {
      if (sequence > 0)
        qual = keyFac.createColumnKey(plasmaType, sequence, metaField);
      else
        qual = keyFac.createColumnKey(plasmaType, metaField);
      ColumnInfo ci = new ColumnInfo(fam, metaField.codeAsBytes(), metaField.getStorageType());
      map.put(ci.getColumn(), ci);
      this.getDelete().addColumn(fam, qual, ci.getDataType(), new byte[0]);

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
    DataType propertyType = DataType.valueOf(property.getType().getName());
    this.getPut().addColumn(fam, qual, propertyType, value);
    this.qualifierMap.add(fam, qual, dataObject, property);

    // if (tableWriter.getTableConfig().optimisticConcurrency())
    // checkAndSetConcurrencyAttribs(dataObject, plasmaType, sequence, property,
    // fam, keyFac,
    // this.getPut());

    if (log.isDebugEnabled())
      log.debug("writing " + Bytes.toString(qual) + " / " + Bytes.toString(value));
  }

  @Override
  public void writeRowData(byte[] fam, byte[] qualifier, DataType valueDataType, byte[] value)
      throws IOException {
    this.getPut().addColumn(fam, qualifier, valueDataType, value);
  }

  @Override
  public void writeRowAttribute(String name, byte[] value) throws IOException {
    // this.getPut().setAttribute(name, value);
  }

  @Override
  public byte[] readRowAttribute(String name) throws IOException {
    // return this.getPut().getAttribute(name);
    return null;
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
    DataType propertyDataType = DataType.valueOf(property.getType().getName());
    // this.getDelete().addColumns(fam, qual, propertyDataType); // deletes all
    // // cell versions
    this.getDelete().addColumn(fam, qual, propertyDataType, null);
    this.qualifierMap.add(fam, qual, dataObject, property);

    // if (tableWriter.getTableConfig().optimisticConcurrency())
    // checkAndSetConcurrencyAttribs(dataObject, plasmaType, sequence, property,
    // fam, keyFac,
    // this.getDelete());

    if (log.isDebugEnabled())
      log.debug("deleting " + Bytes.toString(qual));
  }

  @Override
  public void deleteRowData(byte[] fam, byte[] qualifier) throws IOException {
    // getDelete().addColumns(fam, qualifier); // deletes all cell version
    this.getDelete().addColumn(fam, qualifier, DataType.String, null);
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

    this.getIncrement().addColumn(fam, qual, DataType.Long, Bytes.toBytes(value));
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

  /**
   * For concurrent types, annotate the mutation (row) with bytes data from the
   * concurrency property
   * 
   * @param dataObject
   *          the data object or row entity
   * @param plasmaType
   *          the type
   * @param sequence
   *          the column sequence number
   * @param property
   *          the column property
   * @param columnFamily
   *          the column familty
   * @param columnKeyFactory
   *          the column key fac
   * @param mutation
   *          the Put or Delete mutation
   * @throws IOException
   */
  private void checkAndSetConcurrencyAttribs(PlasmaDataObject dataObject, PlasmaType plasmaType,
      long sequence, PlasmaProperty property, byte[] columnFamily,
      GraphStatefullColumnKeyFactory columnKeyFactory, Mutation mutation) throws IOException {
    ChangeSummary changeSummary = dataObject.getDataGraph().getChangeSummary();
    if (plasmaType.isConcurrent() && !changeSummary.isCreated(dataObject)) {
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
          long longDataValue = DataConverter.INSTANCE.toLong(property.getType(), dataValue);
          byte[] valueBytes = RocksDBDataConverter.INSTANCE.toBytes(property, longDataValue);
          byte[] concurrQual = null;
          if (sequence > 0)
            concurrQual = columnKeyFactory.createColumnKey(plasmaType, sequence, concurrProp);
          else
            concurrQual = columnKeyFactory.createColumnKey(plasmaType, concurrProp);

          // mutation.setAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_FAM_BYTES,
          // columnFamily);
          // mutation.setAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_QUAL_BYTES,
          // concurrQual);
          // mutation.setAttribute(RowWriter.ROW_ATTR_NAME_CONCURRENT_VALUE_BYTES,
          // valueBytes);

          this.getTableWriter().setHasConcurrentRows(true);

          // Increment and set this managed concurrent property
          // unless entire entity is deleted. Note we can delete
          // cells using a Delete mutation but unless the entire
          // entity represented by the given data object is deleted,
          // we still use the Put mutation to increment the version
          // property, as you can't change a cell using Delete,
          // only delete a cell.
          // if (!changeSummary.isDeleted(dataObject)) {
          // longDataValue++;
          // byte[] updatedValueBytes =
          // HBaseDataConverter.INSTANCE.toBytes(property, longDataValue);
          // this.getPut().addColumn(columnFamily, concurrQual,
          // updatedValueBytes);
          // }

          break;
        default:
          throw new GraphServiceException("unsupported concurrent data flavor ("
              + concurrent.getDataFlavor() + ") for property, " + concurrProp + ", with datatype "
              + concurrProp.getType() + "");
        }
      }
    }
  }

}
