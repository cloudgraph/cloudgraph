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
package org.cloudgraph.hbase.mutation;

import java.util.List;

import org.apache.hadoop.hbase.client.Row;
import org.cloudgraph.common.Pair;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;

/**
 * Associates a {@link Row} mutation collection with the qualifiers (as String)
 * mapped to meta data properties used to create the qualifiers, used to quickly
 * map results from update operations (e.g. an HBase Increment) back from the
 * qualifiers given in the client result, in order to populate the returned
 * results back to the originating data object properties.
 * 
 * @author Scott Cinnamond
 * @since 1.0.9
 */
public class Mutations {
  private List<Row> rows;
  private Qualifiers qualifierMap;

  @SuppressWarnings("unused")
  private Mutations() {
    super();
  }

  public Mutations(List<Row> rows, Qualifiers qualifierMap) {
    super();
    this.rows = rows;
    this.qualifierMap = qualifierMap;
  }

  public List<Row> getRows() {
    return rows;
  }

  public Qualifiers getQualifierMap() {
    return qualifierMap;
  }

  public void add(byte[] family, byte[] qualifier, PlasmaDataObject dataObject,
      PlasmaProperty property) {
    qualifierMap.add(family, qualifier, dataObject, property);
  }

  public Pair<PlasmaDataObject, PlasmaProperty> get(byte[] family, byte[] qualifier) {
    return qualifierMap.get(family, qualifier);
  }

}
