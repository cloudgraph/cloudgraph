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

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.mapping.StoreMappingContext;

public class HBaseServiceContext implements ServiceContext {
  private static Log log = LogFactory.getLog(HBaseServiceContext.class);
  private Properties properties;
  private StoreMappingContext mappingContext;

  public HBaseServiceContext() {
    this.properties = new Properties();
    this.mappingContext = new StoreMappingContext(this.properties);
  }

  public HBaseServiceContext(Properties properties) {
    this.properties = properties;
  }

  @Override
  public void close() {
  }

  @Override
  public Properties getProperties() {
    return this.properties;
  }

  @Override
  public StoreMappingContext getStoreMapping() {
    return this.mappingContext;
  }
}
