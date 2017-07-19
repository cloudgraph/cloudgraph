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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.hbase.key.StatefullColumnKeyFactory;
import org.cloudgraph.state.BindingSequenceGenerator;
import org.cloudgraph.state.SequenceGenerator;
import org.cloudgraph.store.key.EntityMetaField;
import org.cloudgraph.store.key.GraphMetaField;
import org.cloudgraph.store.key.GraphStatefullColumnKeyFactory;
import org.cloudgraph.store.service.DuplicateRowException;
import org.cloudgraph.store.service.GraphServiceException;
import org.cloudgraph.store.service.MissingRowException;
import org.cloudgraph.store.service.ToumbstoneRowException;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.ChangeSummary;
import commonj.sdo.DataObject;

/**
 * The operational, configuration and other state information
 * required for write operations on a single graph row.
 * <p>
 * Acts as a single component within a {@link TableWriter} container
 * and encapsulates the HBase client <a target="#" href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html">Put</a> and
 * <a target="#" href="http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html">Put</a> 
 * operations for use in write operations across multiple logical
 * entities within a graph row. 
 * </p>
 * 
 * @see org.cloudgraph.hbase.io.TableWriter
 * @author Scott Cinnamond
 * @since 0.5.1
 */
public class GraphRowWriter extends DefaultRowOperation
    implements RowWriter {

    private static Log log = LogFactory.getLog(GraphRowWriter.class);
	private TableWriter tableWriter;
    private Put row;
    private Delete rowDelete;
    private Map<Integer, EdgeWriter> edgeWriterMap = new HashMap<Integer, EdgeWriter>();

    public GraphRowWriter(byte[] rowKey,
    	DataObject rootDataObject,
    	TableWriter tableWriter) {
		super(rowKey, rootDataObject);
		this.tableWriter = tableWriter;
		this.row = new Put(rowKey);
	}
    
    @Override
	public SequenceGenerator getSequenceMapping() throws IOException {
		if (this.sequenceMapping == null) {
			this.sequenceMapping = createSequenceMapping(this.rowKey, 
				this.rootDataObject, 
				this.rootDataObject.getDataGraph().getChangeSummary());
		}
		return this.sequenceMapping;
	}

    @Override
	public GraphStatefullColumnKeyFactory getColumnKeyFactory() throws IOException  {
    	if (this.columnKeyFactory == null) {
    		this.columnKeyFactory = new StatefullColumnKeyFactory(
    			this);
    	}
		return this.columnKeyFactory;
	}

	@Override
	public Put getRow() {
		return this.row;
	}
	
	@Override
	public void deleteRow() {
		getRowDelete();
	}
	
	/**
	 * Returns the existing (or creates a new) row delete mutation.
	 * @return the existing (or creates a new) row delete mutation.
	 */
	@Override
	public Delete getRowDelete() {
		if (this.rowDelete == null) {
			this.rowDelete = new Delete(this.getRowKey());
		}
		return this.rowDelete;
	}
	
	/**
	 * Returns whether there is an existing row delete mutation.
	 * @return whether there is an existing row delete mutation.
	 */
	@Override
	public boolean hasRowDelete() {
		return this.rowDelete != null;
	}
	
	/**
	 * Returns a single column value for this row given a context
	 * data object and property. Uses a statefull column key factory
	 * to generate a column key based on the given context data object 
	 * and property.
	 * @param dataObject the context data object
	 * @param property the context property
	 * @return the column value bytes
	 * @throws IOException
	 * 
	 * @see StatefullColumnKeyFactory
	 */
	@Override
	public byte[] fetchColumnValue(PlasmaDataObject dataObject, 
			PlasmaProperty property) throws IOException {
    	byte[] qualifier = this.getColumnKeyFactory().createColumnKey(
    		(PlasmaType)dataObject.getType(), property);
    	
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
	 * Returns whether the root data object for this writer
	 * is created. 
	 * @return whether the root data object for this writer
	 * is created.
	 */
	@Override
	public boolean isRootCreated() {
		return this.rootDataObject.getDataGraph().getChangeSummary().isCreated(
				this.rootDataObject);
	}
	
	/**
	 * Returns whether the root data object for this writer
	 * is deleted. 
	 * @return whether the root data object for this writer
	 * is deleted.
	 */
	@Override
	public boolean isRootDeleted() {
		return this.rootDataObject.getDataGraph().getChangeSummary().isDeleted(
				this.rootDataObject);
	}

	@Override
	public List<Row> getWriteOperations() {
		List<Row> result = new ArrayList<>(2);
		// if any qualifiers
		if (this.row != null && this.row.size() > 0)
			result.add(this.row);
		//for delete , can be just the oper with no qualifiers
		if (this.rowDelete != null)
			result.add(this.rowDelete);
		
		return result;
	}
	
	/**
     * Initializes a graph state by querying for a row
     * based on the given row key and either creating a new (empty)
     * graph state for an entirely new graph, or otherwise initializing
     * a graph state based on state or state and management columns in
     * the existing returned row.   
     * 
     * @param rowKey the row key
     * @param dataGraph the data graph
     * @param changeSummary the change summary
     * @return the graph state
     * @throws IOException
     * @throws DuplicateRowException for a new graph if a row already exists
     * for the given row key
     * @throws GraphServiceException where except for a new graph, if no row
     * exists for the given row key
     */
    protected SequenceGenerator createSequenceMapping(byte[] rowKey, 
    		DataObject dataObject,
    		ChangeSummary changeSummary) throws IOException
    {
    	SequenceGenerator graphState;
		// --ensure row exists unless a new row/graph
		// --use empty get with only necessary "state" management columns
		
		// if entirely new graph for the given 
		// distributed or sub-graph root
		if (changeSummary.isCreated(dataObject)) {
			TableConfig tableConfig = this.tableWriter.getTableConfig();
			if (tableConfig.uniqueChecks()) {				
				Result result = getMinimalRow(rowKey, tableConfig, this.tableWriter.getTable());
	    		if (!result.isEmpty()) {
	    			if (!result.containsColumn(
	    					tableConfig.getDataColumnFamilyNameBytes(), 
	    					GraphMetaField.__TSTN__.asBytes())) {
	    			    throw new DuplicateRowException("no row for id '"
	    				    + Bytes.toString(rowKey) + "' expected when creating new row for table '"
	    				    + tableConfig.getTable().getName() + "'"); 
	    			}
	    			else {
	    				if (!tableConfig.tombstoneRowsOverwriteable())
	    			        throw new ToumbstoneRowException("no toumbstone row for id '"
	        				    + Bytes.toString(rowKey) + "' expected when creating new row for table '"
	        				    + tableConfig.getTable().getName() + "' - cannot overwrite toumbstone row"); 
	    			}
	    		}
			}
			PlasmaDataObject root = (PlasmaDataObject)dataObject;
    		graphState = new BindingSequenceGenerator(root.getUUID(),
    			this.tableWriter.getDistributedOperation().getMarshallingContext());
        }
		else {
			TableConfig tableConfig = this.tableWriter.getTableConfig();
			Result result = getStateRow(rowKey, tableConfig, this.tableWriter.getTable());
    		if (result.isEmpty()) {
    			throw new MissingRowException(tableConfig.getTable().getName(),
    				Bytes.toString(rowKey));  
    		}
			if (result.containsColumn(
					tableConfig.getDataColumnFamilyNameBytes(), 
					GraphMetaField.__TSTN__.asBytes())) {
			    throw new ToumbstoneRowException("no row for id '"
    				    + Bytes.toString(rowKey) + "' expected when modifying row for table '"
    				    + tableConfig.getTable().getName() 
    				    + "' - cannot overwrite toumbstone row"); 
			}
    		byte[] state = result.getValue(Bytes.toBytes(tableConfig.getDataColumnFamilyName()), 
    				GraphMetaField.__SMP__.asBytes());
            if (state != null) {
            	if (log.isDebugEnabled()) {
            		byte[] root = result.getValue(Bytes.toBytes(tableConfig.getDataColumnFamilyName()), 
            				GraphMetaField.__RU__.asBytes());
           		    log.debug("root: " + Bytes.toString(root) 
            			+ " state: " + Bytes.toString(state));
            	}
            }
            else
    			throw new OperationException("expected column '"
    				+ GraphMetaField.__SMP__ + " for row " 
    				+ Bytes.toString(rowKey) + "'"); 
            graphState = new BindingSequenceGenerator(Bytes.toString(state), 
            		this.tableWriter.getDistributedOperation().getMarshallingContext());
            
            // Even though we found a row, the user could have committed a data object
            // which was copied and has a different UUID than the original data object
            // which generated the graph state. Subsequent update or delete operations using
            // this mismatched UUID can cause havoc, as the UUID is a key used to look up
            // sequence values from the state and create column keys and modify or delete
            // associated values. 
            PlasmaDataObject root = (PlasmaDataObject)dataObject;
            if (!graphState.getRootUUID().equals(root.getUUID()))
             	throw new UUIDMismatchException("Graph state root UUID '"+graphState.getRootUUID()+"' "
             		+ "does not match writer sub-root, "+root + " - can be caused by data object copy operations, "
             				+ "where only properties are copied not the UUID, then the copied object is modified and comitted");
    	}   		
    	return graphState;
    }
    
    private Result getMinimalRow(byte[] rowKey, TableConfig tableConfig, Table table) throws IOException {
		Get existing = new Get(rowKey);
		existing.addColumn(tableConfig.getDataColumnFamilyNameBytes(), 
				GraphMetaField.__RU__.asBytes());
		existing.addColumn(tableConfig.getDataColumnFamilyNameBytes(), 
				GraphMetaField.__RT__.asBytes());
		existing.addColumn(tableConfig.getDataColumnFamilyNameBytes(), 
				GraphMetaField.__TSTN__.asBytes());		
		return table.get(existing);    	
    }
    
    private Result getStateRow(byte[] rowKey, TableConfig tableConfig, Table table) throws IOException {
		Get existing = new Get(rowKey);
		byte[] fam = tableConfig.getDataColumnFamilyNameBytes();
		for (GraphMetaField field : GraphMetaField.values()) {
			existing.addColumn(fam, field.asBytes());			
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
			    edgeWriter = new EdgeWriter(dataObject,
					property, sequence, this.getTableWriter().getTableConfig(),
					this.graphConfig, 
					this);
			}
			else { // its the root data object
			    edgeWriter = new EdgeWriter(dataObject,
					property, this.getTableWriter().getTableConfig(),
					this.graphConfig, 
					this);
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
		
		PlasmaType plasmaType = (PlasmaType)dataObject.getType();
		byte[] uuidQual = null;
		byte[] typeQual = null;
		if (sequence > 0) {
			uuidQual = this.getColumnKeyFactory().createColumnKey(plasmaType, sequence, EntityMetaField._UU_);
			typeQual = this.getColumnKeyFactory().createColumnKey(plasmaType, sequence, EntityMetaField._TP_);
		}
		else {
			uuidQual = this.getColumnKeyFactory().createColumnKey(plasmaType, EntityMetaField._UU_);
			typeQual = this.getColumnKeyFactory().createColumnKey(plasmaType, EntityMetaField._TP_);
		}
		this.getRow().addColumn(fam, uuidQual, 
				Bytes.toBytes(dataObject.getUUIDAsString()));			
		this.getRow().addColumn(fam, typeQual, 
				encodeType(plasmaType));			
	}
	
	@Override
	public void deleteRowEntityMetaData(PlasmaDataObject dataObject, long sequence) throws IOException {
		byte[] fam = this.getTableWriter().getTableConfig().getDataColumnFamilyNameBytes();
		GraphStatefullColumnKeyFactory keyFac = this.getColumnKeyFactory();
		PlasmaType plasmaType = (PlasmaType)dataObject.getType();
		byte[] qual = null;
		for (EntityMetaField metaField : EntityMetaField.values()) {
			if (sequence > 0) 
				qual = keyFac.createColumnKey(plasmaType, sequence, metaField);
			else 
				qual = keyFac.createColumnKey(plasmaType, metaField);
			this.getRowDelete().addColumn(fam, qual);	
		}
	}
	
	@Override
	public void writeRowData(PlasmaDataObject dataObject, long sequence,
			PlasmaProperty property, byte[] value) throws IOException {
		byte[] fam = this.getTableWriter().getTableConfig().getDataColumnFamilyNameBytes();
		PlasmaType plasmaType = (PlasmaType)dataObject.getType();
		GraphStatefullColumnKeyFactory keyFac = this.getColumnKeyFactory();
		
		byte[] qual = null;
		if (sequence > 0) 
			qual = keyFac.createColumnKey(plasmaType, sequence, property);
		else 
			qual = keyFac.createColumnKey(plasmaType, property);
		this.getRow().addColumn(fam, qual, value);			
        
		if (log.isDebugEnabled())
            log.debug("writing " + Bytes.toString(qual) + " / " + Bytes.toString(value));            	
	}
	
	@Override
	public void deleteRowData(PlasmaDataObject dataObject, long sequence,
			PlasmaProperty property) throws IOException {
		byte[] fam = this.getTableWriter().getTableConfig().getDataColumnFamilyNameBytes();
		PlasmaType plasmaType = (PlasmaType)dataObject.getType();
		GraphStatefullColumnKeyFactory keyFac = this.getColumnKeyFactory();
		
		byte[] qual = null;
		if (sequence > 0) 
			qual = keyFac.createColumnKey(plasmaType, sequence, property);
		else 
			qual = keyFac.createColumnKey(plasmaType, property);
		this.getRowDelete().addColumn(fam, qual);			
        
		if (log.isDebugEnabled())
            log.debug("deleting " + Bytes.toString(qual));            	
	}
	
	
	private Map<DataObject, Long> dataObjectSequenceMap = new HashMap<>();
	
	@Override
	public void addSequence(DataObject dataObject, long sequence) {
		Long existing = this.dataObjectSequenceMap.get(dataObject);
		if (existing != null) 
			 throw new IllegalArgumentException("sequence ("+existing+") already mapped for data object, "
				+ dataObject); 
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
			 throw new IllegalArgumentException("sequence not found for data object, "
				+ dataObject); 
		return result;
	}

	@Override
	public byte[] encodeRootType() {
		return encodeType(this.getRootType());
	}

	@Override
	public byte[] encodeType(PlasmaType type) {
		byte[] uriPhysicalName = type.getURIPhysicalNameBytes(); 
		byte[] uri = type.getURIBytes();
		byte[] physicalName = type.getPhysicalNameBytes();	
		byte[] name = type.getNameBytes();	
		if (uriPhysicalName != null && physicalName != null) {
			return org.cloudgraph.common.Bytes.concat(uriPhysicalName, 
					Bytes.toBytes(ROOT_TYPE_DELIM), physicalName);
		}
		else {
			log.warn("no qualified physical name available for type, " + type
					+ ", encoding qualified logical name - please annotate your model with physical name aliases to facilitate logical/physical name isolation");
		    return org.cloudgraph.common.Bytes.concat(uri, 
		    		Bytes.toBytes(ROOT_TYPE_DELIM), name);
		}
    }

}
