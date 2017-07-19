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
package org.cloudgraph.hbase.service;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.cloudgraph.mapreduce.GraphService;
import org.cloudgraph.state.PooledStateManager;
import org.plasma.common.bind.DefaultValidationEventHandler;
import org.plasma.query.bind.PlasmaQueryDataBinding;
import org.plasma.query.model.From;
import org.plasma.query.model.Query;
import org.plasma.query.model.QueryValidator;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.access.DataGraphDispatcher;
import org.plasma.sdo.access.PlasmaDataAccessService;
import org.plasma.sdo.core.SnapshotMap;
import org.plasma.sdo.helper.PlasmaTypeHelper;
import org.xml.sax.SAXException;

import commonj.sdo.DataGraph;
import commonj.sdo.Type;

/**
 * Top level provider service implementing the
 * {@link org.plasma.sdo.access.DataAccessService DataAccessService } 
 * interface and delegating to {@link GraphQuery} for serving
 * data from HBase back to the client, and {@link GraphDispatcher} for
 * propagating changes to one or more data graphs back to HBase. 
 * <p>
 * CloudGraph&#8482; is based on the Service Data Objects (SDO) 2.1 specification 
 * and is designed as a suite of SDO Data Access Services (DAS) under the 
 * PlasmaSDO&#8482; Service Data Objects implementation. 
 * </p> 
 * <p>
 * Typical CRUD operations are provided across any full or partial 
 * data graph, and any "slice" of a graph or set of
 * sub-graph slices can be returned using 
 * common SDO query mechanisms including XPath and others.     
 * </p>
 * @see org.plasma.sdo.access.DataAccessService
 * @see GraphQuery
 * @see GraphDispatcher
 * @author Scott Cinnamond
 * @since 0.5
 */
public class HBaseGraphService implements PlasmaDataAccessService, GraphService {
    
    private static Log log = LogFactory.getLog(HBaseGraphService.class);
    private ServiceContext context;
    
    public HBaseGraphService() {
    	this.context = new LazyServiceContext(PooledStateManager.getInstance());
    }
 
    public void initialize() {}
    public void close() {
    	this.context.close();
    }
    
    public int count(Query query) {
        if (query == null)
            throw new IllegalArgumentException("expected non-null 'query' argument");
        validate(query);
        if (log.isDebugEnabled()) {
            log(query);
        }
        GraphQuery dispatcher = null;
        try {
            dispatcher = new GraphQuery(this.context);
            return dispatcher.count(query);
        }
        finally {
        	if (dispatcher != null)
        		dispatcher.close();
        }        
    }
    
    public int[] count(Query[] queries) {
        if (queries == null)
            throw new IllegalArgumentException("expected non-null 'queries' argument");
        int[] counts = new int[queries.length];
        for (int i = 0; i < queries.length; i++)
            counts[i] = count(queries[i]);
        return counts;
    }

    public DataGraph[] find(Query query) {
        if (query == null)
            throw new IllegalArgumentException("expected non-null 'query' argument");
        //validate(query);
        if (log.isDebugEnabled()) {
            log(query);
        }
        GraphQuery dispatcher = null;
        try {
            dispatcher = new GraphQuery(this.context);
            Timestamp snapshotDate = new Timestamp((new Date()).getTime());
            return dispatcher.find(query, snapshotDate);
        }
        finally {
        	if (dispatcher != null)
        		dispatcher.close();
        }        
    }

    public DataGraph[] find(Query query, int maxResults) {
        if (query == null)
            throw new IllegalArgumentException("expected non-null 'query' argument");
        validate(query);
        if (log.isDebugEnabled()) {
            log(query);
        }
        GraphQuery dispatcher = null;
        try {
            dispatcher = new GraphQuery(this.context);
            DataGraph[] results = dispatcher.find(query, -1, new Timestamp((new Date()).getTime()));
            return results;
        }
        finally {
        	if (dispatcher != null)
        		dispatcher.close();
        }        
    }

    public List<DataGraph[]> find(Query[] queries) {
        if (queries == null)
            throw new IllegalArgumentException("expected non-null 'queries' argument");
        GraphQuery dispatcher = null;
        try {
            dispatcher = new GraphQuery(this.context);
            List<DataGraph[]> list = new ArrayList<DataGraph[]>();
            Timestamp snapshotDate = new Timestamp((new Date()).getTime());
            for (int i = 0; i < queries.length; i++)
            {
                validate(queries[i]);
                if (log.isDebugEnabled()) {
                    log(queries[i]);
                }
                DataGraph[] results = dispatcher.find(queries[i], snapshotDate);
                list.add(results);
            }
            return list;
        }
        finally {
        	if (dispatcher != null)
        		dispatcher.close();
        }        
    }

    public SnapshotMap commit(DataGraph dataGraph, String username) {
        if (dataGraph == null)
            throw new IllegalArgumentException("expected non-null 'dataGraph' argument");
        if (username == null)
            throw new IllegalArgumentException("expected non-null 'username' argument");
        if (username.trim().length() == 0)
            throw new IllegalArgumentException("unexpected zero length 'username' argument");
        SnapshotMap snapshotMap = new SnapshotMap(new Timestamp((new Date()).getTime()));
        DataGraphDispatcher dispatcher = null;
        try {
            dispatcher = new GraphDispatcher(this.context,
        		snapshotMap, username);
            dispatcher.commit(dataGraph);
            return snapshotMap;
        }
        catch (DataAccessException e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new DataAccessException(e);
        }
        finally {
        	if (dispatcher != null)
        		dispatcher.close();
        }
    }

    public SnapshotMap commit(DataGraph[] dataGraphs, String username) {
        if (dataGraphs == null)
            throw new IllegalArgumentException("expected non-null 'dataGraphs' argument");
        if (username == null)
            throw new IllegalArgumentException("expected non-null 'username' argument");
        if (username.trim().length() == 0)
            throw new IllegalArgumentException("unexpected zero length 'username' argument");
        
        SnapshotMap snapshotMap = new SnapshotMap(new Timestamp((new Date()).getTime()));
        DataGraphDispatcher dispatcher = null;
        
        try {
            dispatcher = new GraphDispatcher(this.context,
            		snapshotMap,
                    username);
            dispatcher.commit(dataGraphs);        
            return snapshotMap;
        }
        catch (DataAccessException e) {
            throw e;
        }
        catch (Exception t) {
            throw new DataAccessException(t);
        }
        finally {
        	if (dispatcher != null)
        		dispatcher.close();
        }
    }

    private void validate(Query query)
    {
        From from = (From)query.getFromClause();
        Type type = PlasmaTypeHelper.INSTANCE.getType(
        		from.getEntity().getNamespaceURI(), 
        		from.getEntity().getName());
        if (log.isDebugEnabled())
        	log.debug("validating query");
        new QueryValidator((Query)query, type);
    }
 
    private void log(Query query)
    {
    	if (log.isDebugEnabled()) {
	    	String xml = "";
	        PlasmaQueryDataBinding binding;
			try {
				binding = new PlasmaQueryDataBinding(
				        new DefaultValidationEventHandler());
		        xml = binding.marshal(query);
			} catch (JAXBException e) {
			} catch (SAXException e) {
			}
	        log.debug("query: " + xml);
    	}
    }

	@Override
	public DataGraph[] find(org.plasma.query.Query query, JobContext context)
			throws IOException {
		 
		return this.find(query.getModel());
	}

	@Override
	public void commit(DataGraph graph, JobContext context) throws IOException {
		String username = "graph-service";
		if (context != null)
			username = context.getJobName();
		this.commit(graph, username);
	}

	@Override
	public void commit(DataGraph[] graphs, JobContext context)
			throws IOException {
		String username = "graph-service";
		if (context != null)
			username = context.getJobName();
		this.commit(graphs, username);
	}

}
