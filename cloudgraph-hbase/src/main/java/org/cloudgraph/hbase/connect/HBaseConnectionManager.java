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
package org.cloudgraph.hbase.connect;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.cloudgraph.config.CloudGraphConfig;
import org.cloudgraph.config.TableConfig;
import org.cloudgraph.hbase.service.CloudGraphContext;
import org.cloudgraph.state.StateException;
import org.cloudgraph.store.service.GraphServiceException;


/**
 * Manages HBase table pool and table interface access.
 * @see CloudGraphContext
 * @see TableConfig
 * @author Scott Cinnamond
 * @since 0.5
 */
public class HBaseConnectionManager {
	
	/** not currently used as connection pools for HBase 1.0.x client not recommended */
	public static final String CONNECTION_POOL_MIN_SIZE = "org.plasma.sdo.access.provider.hbase.ConnectionPoolMinSize";
	public static final String CONNECTION_POOL_MAX_SIZE = "org.plasma.sdo.access.provider.hbase.ConnectionPoolMaxSize";

	private GenericObjectPool<Connection> pool;

	private static final Log log = LogFactory.getLog(HBaseConnectionManager.class);

    private static volatile HBaseConnectionManager instance;
    private Configuration config;
	 

	private HBaseConnectionManager() {		
		this.config = CloudGraphContext.instance().getConfig();
		
		int min = this.config.getInt(CONNECTION_POOL_MIN_SIZE, 1);
		int max = this.config.getInt(CONNECTION_POOL_MAX_SIZE, 20);
		
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMinIdle(min);     
		poolConfig.setMaxTotal(max);
		PooledConnectionFactory factory = new PooledConnectionFactory(this.config);
		this.pool = new GenericObjectPool<Connection>(factory, poolConfig);
		factory.setPool(pool);
	}

	public static HBaseConnectionManager instance()
    {
        if (instance == null)
            initInstance();   
        return instance;     
    }

    private static synchronized void initInstance()
    {
        if (instance == null)
            instance = new HBaseConnectionManager();
    }
 
	protected void finalize() {
		log.debug("Finalizing ConnectionManager");
		try {
			super.finalize();
		} catch (Throwable ex) {
			log.error("ConnectionManager finalize failed to disconnect: ", ex);
		}
	}
	
	public Connection getConnection()  
	{
        try {
			return this.pool.borrowObject();
		} catch (Exception e) {
			throw new GraphServiceException(e);
		}
        finally {
        }
    }
			
	public void createTable(Connection connection, TableName name) {

		Admin admin = null;
    	try {
    		admin = connection.getAdmin();
    		TableConfig tableConfig = CloudGraphConfig.getInstance().getTable(name.getNamespaceAsString(), 
    				name.getNameAsString());
    		HTableDescriptor tableDesc = new HTableDescriptor(name);
	    	HColumnDescriptor fam1 = new HColumnDescriptor(tableConfig.getDataColumnFamilyName().getBytes());
	    	tableDesc.addFamily(fam1);
	    	try {
			    admin.createTable(tableDesc);
	    	}
	    	catch (NamespaceNotFoundException nnf) {
				NamespaceDescriptor namespace =
					NamespaceDescriptor.create(name.getNamespaceAsString())
					    .addConfiguration("Description", "cloudgraph generated namespace").build();
				admin.createNamespace(namespace);
			    admin.createTable(tableDesc);	    		
	    	}
		} catch (MasterNotRunningException e1) {
			throw new StateException(e1);
		} catch (ZooKeeperConnectionException e1) {
			throw new StateException(e1);
		} catch (IOException e) {
			throw new StateException(e);
		} finally {
			if (admin != null)
				try {
					admin.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
		}
    }

	public void deleteTable(Connection connection, TableName name) {

		Admin admin = null;
    	try {
    		admin = connection.getAdmin();
    		admin.disableTable(name);
    		admin.deleteTable(name);
		} catch (MasterNotRunningException e1) {
			throw new StateException(e1);
		} catch (ZooKeeperConnectionException e1) {
			throw new StateException(e1);
		} catch (IOException e) {
			throw new StateException(e);
		} finally {
			if (admin != null)
				try {
					admin.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
		}
    }
	
}
