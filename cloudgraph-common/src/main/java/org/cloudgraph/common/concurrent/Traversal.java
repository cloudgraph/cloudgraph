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
package org.cloudgraph.common.concurrent;

import java.util.List;

import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.provider.common.PropertyPair;

/**
 * Encapsulates minimal recursive graph traversal information for use in
 * concurrent contexts, such as where a traversal is needed but the
 * initialization data must be captured and stored, and a traversal from a given
 * subroot, initiated later.
 * 
 * @author Scott Cinnamond
 * @since 0.6.2
 */
public class Traversal {
  private PlasmaType subrootType;
  private PlasmaDataObject source;
  private PlasmaProperty sourceProperty;
  private List<PropertyPair> childKeyPairs;
  private int level;

  @SuppressWarnings("unused")
  private Traversal() {
  }

  public Traversal(PlasmaType subrootType, PlasmaDataObject source, PlasmaProperty sourceProperty,
      List<PropertyPair> childKeyPairs, int level) {
    super();
    this.subrootType = subrootType;
    this.source = source;
    this.sourceProperty = sourceProperty;
    this.childKeyPairs = childKeyPairs;
    this.level = level;
  }

  public PlasmaType getSubrootType() {
    return subrootType;
  }

  public PlasmaDataObject getSource() {
    return source;
  }

  public PlasmaProperty getSourceProperty() {
    return sourceProperty;
  }

  public List<PropertyPair> getChildKeyPairs() {
    return childKeyPairs;
  }

  public int getLevel() {
    return level;
  }

}
