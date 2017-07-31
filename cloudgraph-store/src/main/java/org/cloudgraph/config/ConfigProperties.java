package org.cloudgraph.config;

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
