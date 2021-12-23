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

import java.util.Collection;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;

public class ConfigProperties extends CompositeConfiguration {
  static final String defaultPropertiesFileName = "cloudgraph.properties";

  public ConfigProperties() {
    super();
    construct();
  }

  public ConfigProperties(Collection configurations) {
    super(configurations);
    construct();
  }

  public ConfigProperties(Configuration inMemoryConfiguration, Collection configurations) {
    super(inMemoryConfiguration, configurations);
    construct();
  }

  public ConfigProperties(Configuration inMemoryConfiguration) {
    super(inMemoryConfiguration);
    construct();
  }

  private void construct() {
    // for (ConfigProperty p : ConfigProperty.values()) {
    // this.addProperty(p.name(), p.getDefaultValue());
    // }
  }

}
