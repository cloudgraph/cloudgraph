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
package org.cloudgraph.rocksdb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.mapping.StoreMapping;
import org.plasma.runtime.DataAccessProviderName;
import org.plasma.runtime.PlasmaRuntime;

/**
 * Provides context information
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class CloudGraphContext {
  private static final Log log = LogFactory.getLog(CloudGraphContext.class);
  private static volatile CloudGraphContext instance;

  private CloudGraphContext() {

    if (log.isDebugEnabled())
      log.debug("creating config...");
    try {

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

  public Object getConfig() {
    return null;
  }

}
