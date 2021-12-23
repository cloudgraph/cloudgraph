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
package org.cloudgraph.aerospike.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.mapping.StoreMapping;
import org.plasma.runtime.DataAccessProviderName;
import org.plasma.runtime.PlasmaRuntime;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

/**
 * Provides context information
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CloudGraphContext {
  private static final Log log = LogFactory.getLog(CloudGraphContext.class);
  private static volatile CloudGraphContext instance;
  private ClientPolicy policy;
  private Host[] hosts;

  private CloudGraphContext() {

    if (log.isDebugEnabled())
      log.debug("creating config...");
    try {
      policy = new ClientPolicy();
      policy.authMode = AuthMode.EXTERNAL_INSECURE;
      // policy.tlsPolicy = TlsPolicy.;

      this.hosts = Host.parseHosts("192.168.1.10", 3000);

    } catch (Exception e) {
      log.error("Error when attempting to connect to DB ", e);
    } finally {
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

  public Host[] getHosts() {
    return hosts;
  }

  public ClientPolicy getConfig() {
    return policy;
  }

}
