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
package org.cloudgraph.rdb.connect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.runtime.ConfigurationConstants;
import org.plasma.runtime.DataAccessProviderName;
import org.plasma.runtime.PlasmaRuntime;
import org.plasma.runtime.Property;
import org.plasma.sdo.access.DataAccessException;

/**
 * Instantiates a provider based on configuration setting and supplies singleton
 * access to the shared provider.
 */
public class ProviderManager {

  private static final Log log = LogFactory.getLog(ProviderManager.class);
  private static ProviderManager instance;
  private ConnectionProvider provider;

  private ProviderManager() {
    Properties props = new Properties();
    for (Property property : PlasmaRuntime.getInstance()
        .getDataAccessProvider(DataAccessProviderName.JDBC).getProperties()) {
      props.put(property.getName(), property.getValue());
    }
    String providerName = props.getProperty(ConfigurationConstants.JDBC_PROVIDER_NAME);
    try {
      this.provider = (ConnectionProvider) java.lang.Class.forName(providerName).newInstance();
    } catch (Exception e2) {
      log.error("Error when attempting to obtain JDBC Provider: " + providerName, e2);
      throw new DataAccessException(e2);
    }
  }

  public static ProviderManager instance() {
    if (instance == null)
      initInstance(); // double-checked locking pattern
    return instance;
  }

  private static synchronized void initInstance() {
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
