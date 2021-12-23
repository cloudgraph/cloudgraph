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
package org.cloudgraph.hbase.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.cloudgraph.store.mapping.StoreMapping;
import org.plasma.runtime.DataAccessProviderName;
import org.plasma.runtime.PlasmaRuntime;

/**
 * Provides context information for HBase such as an already initialized HBase
 * <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/HBaseConfiguration"
 * target="#">configuration</a> instance as well as other elements. The HBase
 * configuration is setup with values taken first from the PlasmaSDO NoSQL DAS
 * <a href="http://docs.plasma-sdo.org/api/org/plasma/config/PlasmaConfig.html"
 * target="#">configuration</a> for HBase then overridden where key matches
 * exist from the CloudGraph {@link org.cloudgraph.store.mapping.StoreMapping
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

    String oldFactory = System.getProperty("javax.xml.parsers.DocumentBuilderFactory");
    System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
        "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");

    if (log.isDebugEnabled())
      log.debug("creating config...");
    try {
      config = HBaseConfiguration.create();
      config.clear();
      // set DAS properties
      for (org.plasma.runtime.Property property : PlasmaRuntime.getInstance()
          .getDataAccessProvider(DataAccessProviderName.HBASE).getProperties()) {
        config.set(property.getName(), property.getValue());
      }
      // override plasma DAS properties where matches exits
      for (org.cloudgraph.store.mapping.Property property : StoreMapping.getInstance()
          .getProperties()) {
        config.set(property.getName(), property.getValue());
      }

    } catch (Exception e) {
      log.error("Error when attempting to connect to DB ", e);
    } finally {
      if (oldFactory != null)
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory", oldFactory);
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
