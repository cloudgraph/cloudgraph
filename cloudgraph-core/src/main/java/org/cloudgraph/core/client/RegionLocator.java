package org.cloudgraph.core.client;

import java.io.IOException;
import java.util.List;

public interface RegionLocator {
  public List<RegionLocation> getAllRegionLocations() throws IOException;
}
