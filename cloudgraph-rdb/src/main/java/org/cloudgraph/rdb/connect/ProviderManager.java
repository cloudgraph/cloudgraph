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
package org.cloudgraph.rdb.connect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.config.ConfigurationConstants;
import org.plasma.config.DataAccessProviderName;
import org.plasma.config.PlasmaConfig;
import org.plasma.config.Property;
import org.plasma.sdo.access.DataAccessException;

/**
 * Instantiates a provider based on configuration setting and supplies singleton access 
 * to the shared provider.
 */
public class ProviderManager {

	private static final Log log = LogFactory.getLog(ProviderManager.class);
	private static ProviderManager instance;
	private ConnectionProvider provider;
	
	private ProviderManager() {		
		Properties props = new Properties();
	    for (Property property : PlasmaConfig.getInstance().getDataAccessProvider(DataAccessProviderName.JDBC).getProperties()) {
	    	props.put(property.getName(), property.getValue());
	    }
	    String providerName = props.getProperty(ConfigurationConstants.JDBC_PROVIDER_NAME);
		try {
			this.provider = (ConnectionProvider)java.lang.Class.forName(providerName).newInstance();
		} catch (Exception e2) {
			log.error("Error when attempting to obtain JDBC Provider: " + providerName, e2);
			throw new DataAccessException(e2);
		}
	}

	public static ProviderManager instance()
    {
        if (instance == null)
            initInstance(); // double-checked locking pattern 
        return instance;     
    }

    private static synchronized void initInstance()
    {
        if (instance == null)
            instance = new ProviderManager();
    }
 
	protected void finalize() {
		log.debug("Finalizing " + this.getClass().getName());
		try {
			super.finalize();
		} catch (Throwable ex) {
			log.error("finalize failed: ", ex);
		}
	}

	public Connection getConnection() throws SQLException {
		return this.provider.getConnection();
	}


}
