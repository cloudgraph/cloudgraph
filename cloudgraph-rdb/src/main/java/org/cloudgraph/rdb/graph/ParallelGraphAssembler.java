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
package org.cloudgraph.rdb.graph;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.common.concurrent.ConfigProps;
import org.cloudgraph.common.concurrent.GraphMetricVisitor;
import org.cloudgraph.common.concurrent.SubgraphTask;
import org.cloudgraph.common.concurrent.Traversal;
import org.cloudgraph.rdb.filter.RDBStatementExecutor;
import org.cloudgraph.rdb.filter.RDBStatementFactory;
import org.cloudgraph.store.lang.DefaultAssembler;
import org.cloudgraph.store.lang.LangStoreGraphAssembler;
import org.plasma.query.collector.SelectionCollector;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.core.CoreNode;

import commonj.sdo.DataGraph;
import commonj.sdo.Property;

/**
* Constructs a data graph in parallel starting with a given root SDO type based on
* a given <a target="#" href="http://plasma-sdo.org/org/plasma/query/collector/Selection.html">"selection graph"</a>, where processing
* proceeds as a breadth-first traversal and tasks/threads are dynamically added based on availability
* within a shared <a href="https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor.html">thread pool</a>. 
* <p>
* While the result graph may be of any arbitrary size or depth, because the traversal is breadth-first, 
* many tasks are typically spawned at the "base" of the graph, exhausting the available pool
* threads. Each subgraph task can spawn further sub tasks based on thread availability, but typically 
* this means each task will traverse and process a healthy segment of the total graph. Since
* the actual size or depth of the result graph is not known until discovered on traversal, a fixed
* number of parallel tasks cannot be initially created, but must be dynamically spawned during graph discovery. 
* <p>
* The assembly is triggered by calling the 
* {@link GraphAssembler#assemble(List<PropertyPair> results)} method which initializes
* the graph root and begins a breadth first traversal of the selection graph as
* represented in the underlying data store. 
* <p>
* Various metrics for the assembly are collected using {@link GraphMetricVisitor} and
* are available as SDO instance properties. 
*  
* @see org.plasma.query.collector.Selection 
* @see ParallelSubgraphTask 
* @see GraphMetricVisitor
* 
* @author Scott Cinnamond
* @since 0.6.2
*/
public class ParallelGraphAssembler extends DefaultAssembler
    implements LangStoreGraphAssembler {

    private static Log log = LogFactory.getLog(ParallelGraphAssembler.class);
    private ThreadPoolExecutor executorService;	
    private ConfigProps config;
	
    /**
 	 * Constructor.
	 * 
	 * @param rootType
	 *            the SDO root type for the result data graph
	 * @param collector
	 *            selected SDO properties. Properties are mapped by selected
	 *            types required in the result graph.
	 * @param snapshotDate
	 *            the query snapshot date which is populated into every data
	 *            object in the result data graph.
	 * @param minPoolSize the minimum or core size of the underlying thread pool used for
	 * all tasks executed under this assembler           
	 * @param maxPoolSize the maximum size of the underlying thread pool used for
	 * all tasks executed under this assembler           
     * @param con
     */
	public ParallelGraphAssembler(PlasmaType rootType, SelectionCollector collector,
			Timestamp snapshotDate, ConfigProps config, Connection con) {
		super(rootType, collector, 
			new RDBStatementFactory(), new RDBStatementExecutor(con),
			new ConcurrentHashMap<Integer, PlasmaDataObject>(),
			snapshotDate);		 
		this.executorService = new ThreadPoolExecutor(
				config.getMinThreadPoolSize(), config.getMaxThreadPoolSize(),
	            0L, TimeUnit.MILLISECONDS,
	            new LinkedBlockingQueue<Runnable>(),
	            new ThreadPoolExecutor.CallerRunsPolicy());
		this.config = config;
	}
	
	public ThreadPoolExecutor getExecutorService() {
		return executorService;
	}
	
	public ConfigProps getConfig() {
		return config;
	}

	@Override
	protected void link(PlasmaDataObject target, PlasmaDataObject source,
			PlasmaProperty sourceProperty) {
		synchronized (source) {
			synchronized (target) {
			    super.link(target, source, sourceProperty);
			}
		}
	}
	
	@Override
	protected PlasmaDataObject createDataObject(List<PropertyPair> row,
			PlasmaDataObject source, PlasmaProperty sourceProperty) {
		synchronized (this) {
			return super.createDataObject(row, source, sourceProperty);
		}
	}
	
	/**
     * Recursively re-constitutes a data graph distributed across multiple
     * tables and/or rows, starting with the given result row. 
     * <p>
     * To retrieve the graph use {@link GraphAssembler#getDataGraph()}.
     * </p>
	 * @param results the result row.
	 */
	@Override
	public void assemble(List<PropertyPair> results)  {
		
    	long before = System.currentTimeMillis();

    	DataGraph dataGraph = initRoot(results);		
    	CoreNode rootNode = (CoreNode)dataGraph.getRootObject();
        
        List<Traversal> traversals = new ArrayList<Traversal>();
        
		// singular reference props
		for (PropertyPair pair : results) {
			if (pair.getProp().isMany() || pair.getProp().getType().isDataType())
			    continue;
			List<PropertyPair> childKeyProps = this.getChildKeyPairs(pair);
			
			Traversal trav = new Traversal((PlasmaType)pair.getProp().getType(), 
					(PlasmaDataObject)this.root,
					pair.getProp(), childKeyProps, 1);
			traversals.add(trav);					
		}
		
		// multi reference props (not found in results)
		Set<Property> props = this.collector.getProperties(this.rootType);
		for (Property p : props) {
			PlasmaProperty prop = (PlasmaProperty)p;
			if (prop.isMany() && !prop.getType().isDataType()) {
				List<PropertyPair> childKeyProps = this.getChildKeyPairs(root, prop);
				Traversal trav = new Traversal((PlasmaType)prop.getType(), 
						(PlasmaDataObject)this.root,
						prop, childKeyProps,
						1);
				traversals.add(trav);					
			}
		}
		
		// create concurrent tasks based on pool availability
		logPoolStatistics();
		int available = numThreadsAvailable();
		if (available > traversals.size())
			available = traversals.size();
		List<SubgraphTask> concurrentTasks = new ArrayList<SubgraphTask>();
		for (int i = 0; i < available; i++) {
			Traversal trav = traversals.get(i);
			SubgraphTask task = new ParallelSubgraphTask(
					trav.getSubrootType(),
					trav.getSource(),
					this.collector,
					this.getStatementFactory(), this.getStatementExecutor(),
					trav.getSourceProperty(), trav.getChildKeyPairs(),
					trav.getLevel(), i, this);
			concurrentTasks.add(task);
		}
		
		// start any asynchronous assemblers
		for (SubgraphTask task : concurrentTasks)
			task.start();
		for (SubgraphTask task : concurrentTasks)
			task.join();

		// add remainder 
		// continue with traversals for this thread
		for (int i = available; i < traversals.size(); i++) {
			Traversal trav = traversals.get(i);
		    ParallelSubgraphTask task = new ParallelSubgraphTask(
			    	trav.getSubrootType(),
			    	trav.getSource(),
					this.collector,
					this.getStatementFactory(), this.getStatementExecutor(),
					trav.getSourceProperty(), trav.getChildKeyPairs(),
					trav.getLevel(), traversals.size(), this);
			task.assemble(); // this thread
		}
		
        if (log.isDebugEnabled())
        	log.debug("completed root " + this.root);
		
    	long after = System.currentTimeMillis();
    	
    	rootNode.getValueObject().put(
    		CloudGraphConstants.GRAPH_ASSEMBLY_TIME,
    		Long.valueOf(after - before));    	
    	
    	GraphMetricVisitor visitor = new GraphMetricVisitor();
    	this.root.accept(visitor);
    	
    	rootNode.getValueObject().put(
        		CloudGraphConstants.GRAPH_NODE_COUNT,
        		Long.valueOf(visitor.getCount()));
    	rootNode.getValueObject().put(
        		CloudGraphConstants.GRAPH_DEPTH,
        		Long.valueOf(visitor.getDepth()));
    	rootNode.getValueObject().put(
        		CloudGraphConstants.GRAPH_THREAD_COUNT,
        		Long.valueOf(visitor.getThreadCount()));
	}

	@Override
	protected void assemble(PlasmaType targetType, PlasmaDataObject source,
			PlasmaProperty sourceProperty, List<PropertyPair> childKeyPairs,
			int level) {
		//noop - 
	}
	
	
	public void logPoolStatistics() {
		if (log.isDebugEnabled())
			log.debug("active: " + executorService.getActiveCount() + ", size: " + executorService.getPoolSize());		
	}
	
	public boolean threadsAvailable() {
		return executorService.getActiveCount() < executorService.getMaximumPoolSize();		
	}
	
	public int numThreadsAvailable() {
		int result = executorService.getMaximumPoolSize() - executorService.getActiveCount();
		if (result < 0)
			result = 0;
		return result;		
	}

}
