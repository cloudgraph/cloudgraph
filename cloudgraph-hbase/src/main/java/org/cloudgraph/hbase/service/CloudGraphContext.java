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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.cloudgraph.config.CloudGraphConfig;
import org.plasma.config.DataAccessProviderName;
import org.plasma.config.PlasmaConfig;

/**
 * Provides context information for HBase such as an already initialized HBase
 * <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/HBaseConfiguration"
 * target="#">configuration</a> instance as well as other elements. The HBase
 * configuration is setup with values taken first from the PlasmaSDO NoSQL DAS
 * <a href="http://docs.plasma-sdo.org/api/org/plasma/config/PlasmaConfig.html"
 * target="#">configuration</a> for HBase then overridden where key matches
 * exist from the CloudGraph {@link org.cloudgraph.config.CloudGraphConfig
 * configuration}. Used by
 * {@link org.cloudgraph.hbase.connect.HBaseConnectionManager} when creating
 * pooled HTable client instances.
 * 
 * @see org.cloudgraph.hbase.connect.HBaseConnectionManager
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CloudGraphContext {
	private static final Log log = LogFactory.getLog(CloudGraphContext.class);
	private static volatile CloudGraphContext instance;
	private Configuration config;

	private CloudGraphContext() {

		String oldFactory = System
				.getProperty("javax.xml.parsers.DocumentBuilderFactory");
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
				"com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");

		if (log.isDebugEnabled())
			log.debug("creating config...");
		try {
			config = HBaseConfiguration.create();
			config.clear();
			// set DAS properties
			for (org.plasma.config.Property property : PlasmaConfig
					.getInstance()
					.getDataAccessProvider(DataAccessProviderName.HBASE)
					.getProperties()) {
				config.set(property.getName(), property.getValue());
			}
			// override plasma DAS properties where matches exits
			for (org.cloudgraph.config.Property property : CloudGraphConfig
					.getInstance().getProperties()) {
				config.set(property.getName(), property.getValue());
			}

		} catch (Exception e) {
			log.error("Error when attempting to connect to DB ", e);
		} finally {
			if (oldFactory != null)
				System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
						oldFactory);
		}
	}

	public static CloudGraphContext instance() {
		if (instance == null)
			initInstance();
		return instance;
	}

	private static synchronized void initInstance() {
		if (instance == null)
			instance = new CloudGraphContext();
	}

	public Configuration getConfig() {
		return config;
	}

}
