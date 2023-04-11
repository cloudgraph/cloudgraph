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
package org.cloudgraph.store.mapping;

import java.util.Properties;

/**
 * Request oriented context information used under both read and write operation
 * to inform various store mapping logic, e.g. the volume-path qualified name of
 * a table determined at runtime.
 * 
 * @author Scott Cinnamond
 * @since 1.1.9
 */
public class StoreMappingContext {
  private Properties properties;

  @SuppressWarnings("unused")
  private StoreMappingContext() {
  }

  public StoreMappingContext(Properties properties) {
    this.properties = properties;
  }

  public boolean hasTableNamespaceRoot() {
    return this.properties.containsKey(ConfigurationProperty.CLOUDGRAPH___TABLE___NAMESPACE___ROOT
        .value());
  }

  public String getTableNamespaceRoot() {
    return this.getProperty(ConfigurationProperty.CLOUDGRAPH___TABLE___NAMESPACE___ROOT.value());
  }

  public boolean hasTableVolumeName() {
    return this.properties.containsKey(ConfigurationProperty.CLOUDGRAPH___TABLE___VOLUME___NAME
        .value());
  }

  public String getTableVolumeName() {
    return this.getProperty(ConfigurationProperty.CLOUDGRAPH___TABLE___VOLUME___NAME.value());
  }

  public boolean hasTablePhysicalNamespaceDelim() {
    return this.properties
        .containsKey(ConfigurationProperty.CLOUDGRAPH___TABLE___PHYSICAL___NAMESPACE___DELIM
            .value());
  }

  public String getTablePhysicalNamespaceDelim() {
    return this.getProperty(ConfigurationProperty.CLOUDGRAPH___TABLE___PHYSICAL___NAMESPACE___DELIM
        .value());
  }

  public Object setProperty(String key, String value) {
    return this.properties.setProperty(key, value);
  }

  public String getProperty(String key) {
    return this.properties.getProperty(key);
  }

}
