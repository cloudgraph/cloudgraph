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
package org.cloudgraph.hbase.graph;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudgraph.common.concurrent.ConfigProps;
import org.cloudgraph.common.concurrent.SubgraphTask;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.hbase.io.DistributedReader;
import org.cloudgraph.hbase.io.EdgeReader;
import org.cloudgraph.hbase.io.OperationException;
import org.cloudgraph.hbase.io.RowReader;
import org.cloudgraph.hbase.io.TableReader;
import org.cloudgraph.store.key.GraphMetaField;
import org.cloudgraph.store.service.GraphServiceException;
//import org.cloudgraph.state.GraphState.Edge;
import org.plasma.query.collector.Selection;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;

import commonj.sdo.Property;

/**
 * A concurrent assembly task which assembles a sub-graph  
 * starting with a given "sub root" based on the
 * given <a target="#"
 * href="http://plasma-sdo.org/org/plasma/query/collector/Selection.html">"selection graph"</a>.
 * Processing proceeds as a breadth-first
 * traversal and additional tasks are dynamically spawned based on thread availability
 * within a shared <a href=
 * "https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor.html"
 * >thread pool</a>. If thread availability is exhausted, processing proceeds
 * within the current thread. 
 *  
 * @see DistributedReader
 * @see RowReader
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
//package protection
class ParallelSubgraphTask extends DefaultSubgraphTask implements SubgraphTask {
    private static Log log = LogFactory.getLog(ParallelSubgraphTask.class);
    
	/**
	 * Constructor. 
     * @param subroot the graph sub root
     * @param selection the graph selection
     * @param snapshotDate the snapshot date
     * @param distributedReader the distributed reader
     * @param source the source data object representing the source edge
     * @param sourceProperty the source property representing the source edge
     * @param rowReader the row reader
     * @param level the traversal level
     * @param taskSequence the task sequence
     * @param executorService the thread pool reference
	 */
	public ParallelSubgraphTask(PlasmaDataObject subroot,
			long subrootSequence,
			Selection selection,
			Timestamp snapshotDate,
			DistributedReader distributedReader,
			EdgeReader collection,
			PlasmaDataObject source,
			PlasmaProperty sourceProperty,
			RowReader rowReader,
			int level, int taskSequence,
			ThreadPoolExecutor executorService,
			ConfigProps config) {
		super(subroot,subrootSequence,selection,snapshotDate,distributedReader,collection,source,sourceProperty,rowReader,
				level,taskSequence, executorService, config);
	}
	
    /**
     * Factory method creating a new task.   
     * @param subroot the graph sub root
     * @param selection the graph selection
     * @param snapshotDate the snapshot date
     * @param distributedReader the distributed reader
     * @param source the source data object representing the source edge
     * @param sourceProperty the source property representing the source edge
     * @param rowReader the row reader
     * @param level the traversal level
     * @param sequence the task sequence
     * @param executorService the thread pool reference
     * @return the task
     */
	@Override
	protected SubgraphTask newTask(PlasmaDataObject subroot,
			long subrootSequence,
			Selection selection, Timestamp snapshotDate,
			DistributedReader distributedReader, EdgeReader collection,PlasmaDataObject source,
			PlasmaProperty sourceProperty, RowReader rowReader, int level,
			int sequence, ThreadPoolExecutor executorService, ConfigProps config) {
		return new ParallelSubgraphTask(subroot,subrootSequence,selection,snapshotDate,distributedReader,collection,source,sourceProperty,rowReader,
				level,sequence, executorService, config);
	}

	@Override
	protected void assemble(PlasmaDataObject target, long targetSequence, EdgeReader sourceCollection, PlasmaDataObject source,
			PlasmaProperty sourceProperty, RowReader rowReader, int level)
			throws IOException {
		
		Set<Property> props = this.getProperties(target, source, sourceProperty, level);
		if (props.size() == 0) 
			return;
        if (log.isDebugEnabled())
			log.debug("assembling("+level+"): " + target.toString() + ": " + props.toString());
        
        // synchronize on row-reader here rather than target because row-reader
        // uses shared column key factory
        synchronized (rowReader) {
		    assembleData(target, targetSequence, props, rowReader);	
        }
		
		TableReader tableReader = rowReader.getTableReader();
		TableConfig tableConfig = tableReader.getTableConfig();
		
		traversals.clear();
	 
		// reference props
		for (Property p : props) {
			PlasmaProperty prop = (PlasmaProperty)p;
			if (prop.getType().isDataType())
				continue;

			EdgeReader edgeReader = null;
			//if (level > 0) {
			if (rowReader.edgeExists((PlasmaType)target.getType(), prop, targetSequence)) {
				edgeReader = rowReader.getEdgeReader((PlasmaType)target.getType(), 
						prop, targetSequence);
			}
			else 
				continue; // edge not found in data
			
			if (!edgeReader.isExternal()) {
				assembleEdges(target, targetSequence, prop, edgeReader, rowReader, 
						tableReader, rowReader, level);
			}
			else {
//				String childTable = rowReader.getGraphState().getRowKeyTable(edges[0].getUuid());
//				if (childTable == null)
//					throw new OperationException("no table found for type, " + 
//							edges[0].getType());
//				TableReader externalTableReader = distributedReader.getTableReader(childTable);
				TableReader externalTableReader = distributedReader.getTableReader(edgeReader.getTable());
				if (externalTableReader == null)
					throw new OperationException("no table reader found for type, " + 
							edgeReader.getBaseType());
				assembleExternalEdges(target, targetSequence, prop, edgeReader, rowReader,
						externalTableReader, level);			
			}
		}		
		
		traverse(level);		
	}
	
	protected void assembleEdges(PlasmaDataObject target, long targetSequence, PlasmaProperty prop, 
			EdgeReader collection, RowReader rowReader, 
		TableReader childTableReader, RowReader childRowReader,
		int level) throws IOException 
	{
		for (long childSequence : collection.getSequences()) {
			PlasmaType subType =  collection.getSubType();
			if (subType == null)
				subType = collection.getBaseType();
			
        	if (childRowReader.contains(childSequence, subType))
        	{            		
        		// we've seen this child before so his data is complete, just link 
        		PlasmaDataObject existingChild = (PlasmaDataObject)childRowReader.getDataObject(childSequence, collection.getBaseType());
    		    synchronized (existingChild) {
    		        synchronized (target) {
        		        link(existingChild, target, prop);
    	            }
        	    }
        		continue; 
        	}
        	
			if (log.isDebugEnabled())
				log.debug("local edge: " 
			        + target.getType().getURI() + "#" +target.getType().getName()
			        + "->" + prop.getName() + " (" + childSequence + ")");
			
//			PlasmaDataObject child = null;
//	    	synchronized (target) {
//        	    // create a child object
//			    child = createChild(target, prop, edge);
//	    	}
//			synchronized (childRowReader) {
//                childRowReader.addDataObject(child);
//			}
//		    synchronized (this.distributedReader) {
//		        this.distributedReader.mapRowReader(child, 
//					childRowReader);	
//		    }
		    
		    synchronized (this.distributedReader) {
			    this.distributedReader.mapRowReader(childSequence, subType,
					childRowReader);					
		    }
	        
			if (log.isDebugEnabled())
				log.debug("traverse: (" + sourceProperty.getName() + ") ");					

			PlasmaDataObject child = null;
	    	synchronized (target) {
	            child = createChild(childSequence, collection, 
	        		source, sourceProperty, childRowReader, this.graph);
		    }
	        
			synchronized (childRowReader) {
	            childRowReader.addDataObject(child);   	
		    }
		    
		    
		    // indicate a non-concurrent traversal given this 
		    // is not a slice assembler no fetch occurring for internal edges
			traversals.add(new Traversal(child, childSequence, collection,
					target, prop, childRowReader,
					false, // indicate a non-concurrent traversal  
					level+1));					
		}
	}
	
	/**
	 * Assembles a given set of edges where the target is a different row, within this table or 
	 * another. Since we are assembling a graph, and each edge links another row, each edge requires
	 * a new row reader.  
	 * @param target the object source to which we link edges
	 * @param prop the edge property
	 * @param edges the edges
	 * @param rowReader the row reader
	 * @param childTableReader the table reader for the child objects
	 * @param level the assembly level
	 * @throws IOException
	 */
	protected void assembleExternalEdges(PlasmaDataObject target, long targetSequence, PlasmaProperty prop, 
			EdgeReader collection, RowReader rowReader, TableReader childTableReader, int level) throws IOException 
	{
		for (String childRowKey : collection.getRowKeys()) {
			Result childResult = null;
			
			// need to look up an existing row reader based on the root UUID of the external graph
			// or the row key, and the row key is all we have in the local graph state. The edge UUID
			// is a local graph UUID. 
			//childRowKey = rowReader.getGraphState().getRowKey(edge.getUuid()); // use local edge UUID
			//String childRowKeyStr = Bytes.toString(childRowKey);
			
			// see if this row is locked during fetch, and wait for it
			Object rowLock = fetchLocks.get(childRowKey);
			if (rowLock != null) {
				synchronized (rowLock) {
					try {
						rowLock.wait();
					} catch (InterruptedException e) {
						log.error(e.getMessage(), e);
					}
				}
			}
			
			RowReader existingChildRowReader = childTableReader.getRowReader(childRowKey);
        	if (existingChildRowReader != null)
        	{      
        		// If assembled this row root before, 
        		// just link it. The data is already complete.
        		PlasmaDataObject existingChild = (PlasmaDataObject)existingChildRowReader.getRootDataObject();
    		    synchronized (existingChild) {
    		        synchronized (target) {
        		        link(existingChild, target, prop);
    	            }
        	    }
    		    continue; 
        	}   
 			
        	// While fetching this node, another thread can fail to find an existing row reader registered
        	// above and fall through to this fetch, and therefore fetch the same row, in addition
        	// to attempting to create the same row reader below, causing an error or warning
        	// The second thread may be arriving at this node from another property/edge and
        	// therefore need to link from another edge above. 
           	fetchLocks.put(childRowKey, new Object());

           	if (log.isDebugEnabled())
				log.debug("fetch external row: " 
			        + prop.toString() + " (" + childRowKey + ")");
			
			childResult = fetchGraph(Bytes.toBytes(childRowKey), childTableReader, collection.getBaseType());
			
	    	if (childResult.containsColumn(rootTableReader.getTableConfig().getDataColumnFamilyNameBytes(), 
	    			GraphMetaField.__TSTN__.asBytes())) {
	    		log.warn("ignoring toubstone result row '" + 
	    				childRowKey + "'");
				continue; // ignore toumbstone edge
	    	}
	    	
	        // need to reconstruct the original graph, so need original UUID
			if (log.isDebugEnabled())
				log.debug("external edge: " 
			        + target.getType().getURI() + "#" +target.getType().getName()
			        + "->" + prop.getName() + " (" + childRowKey + ")");
      	
			
			
			
//			PlasmaDataObject child = null;
//	    	synchronized (target) {
//        	    // create a child object using UUID from external row root
//			    child = createChild(target, prop, edge, uuid);
//	    	}
//	    	
//			RowReader childRowReader = null;
//		    synchronized (childTableReader) {
//		        childRowReader = childTableReader.createRowReader(
//				        child, childResult);
//		    }
//		    synchronized (this.distributedReader) {
//		        this.distributedReader.mapRowReader(child, 
//					childRowReader);	
//		    }
		    
		    
	        // need to reconstruct the original graph, so need original UUID
			byte[] rootUuid = childResult.getValue(Bytes.toBytes(
					childTableReader.getTableConfig().getDataColumnFamilyName()), 
	                GraphMetaField.__RU__.asBytes());
			if (rootUuid == null)
				throw new GraphServiceException("expected column: "
					+ childTableReader.getTableConfig().getDataColumnFamilyName() + ":"
					+ GraphMetaField.__RU__);
			String uuidStr = null;
			uuidStr = new String(rootUuid, 
					childTableReader.getTableConfig().getCharset());
			UUID uuid = UUID.fromString(uuidStr);	    	
			
			PlasmaDataObject child = null;
			synchronized (target) {
	    	// create a child object using UUID from external row root
			    child = createChild(source, sourceProperty, uuid, collection.getBaseType());
			}
			
			RowReader childRowReader = null;
			synchronized (childTableReader) {
			// create a row reader for every external edge
			    childRowReader = childTableReader.createRowReader(
				child, childResult);
			}
			synchronized (this.distributedReader) {
			    this.distributedReader.mapRowReader(childRowKey,
					childRowReader);					
			}
			
			synchronized (target) {
	            childRowReader.addDataObject(child);   	
			}
		    
			//FIXME: we have the child already why is the sequence needed
			traversals.add(new Traversal(child, -1, collection,
					target, prop, childRowReader,
					true,
					level+1));					
	     
		    rowLock = fetchLocks.remove(childRowKey);
		    synchronized (rowLock) {
		    	rowLock.notifyAll();
		    }
		}
	}			
}
