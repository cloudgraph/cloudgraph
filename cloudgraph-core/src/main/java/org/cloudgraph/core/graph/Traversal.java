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
package org.cloudgraph.core.graph;

import org.cloudgraph.core.io.EdgeReader;
import org.cloudgraph.core.io.RowReader;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;

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
  private PlasmaDataObject subroot;
  private long subrootSequence;
  private EdgeReader collection;
  private PlasmaDataObject source;
  private PlasmaProperty sourceProperty;
  private RowReader rowReader;
  private int level;
  private boolean concurrent;

  @SuppressWarnings("unused")
  private Traversal() {
  }

  public Traversal(PlasmaDataObject subroot, long subrootSequence, EdgeReader collection,
      PlasmaDataObject source, PlasmaProperty sourceProperty, RowReader rowReader,
      boolean concurrent, int level) {
    super();
    this.subroot = subroot;
    this.subrootSequence = subrootSequence;
    this.collection = collection;
    this.source = source;
    this.sourceProperty = sourceProperty;
    this.rowReader = rowReader;
    this.concurrent = concurrent;
    this.level = level;
  }

  public PlasmaDataObject getSubroot() {
    return subroot;
  }

  public long getSubrootSequence() {
    return subrootSequence;
  }

  public EdgeReader getCollection() {
    return collection;
  }

  public PlasmaDataObject getSource() {
    return source;
  }

  public PlasmaProperty getSourceProperty() {
    return sourceProperty;
  }

  public RowReader getRowReader() {
    return rowReader;
  }

  public int getLevel() {
    return level;
  }

  public boolean isConcurrent() {
    return concurrent;
  }

}
