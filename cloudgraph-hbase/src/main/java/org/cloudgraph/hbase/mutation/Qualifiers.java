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

import java.util.HashMap;
import java.util.Map;

import org.cloudgraph.common.Pair;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaProperty;

import com.google.common.primitives.Bytes;

/**
 * 
 * @author Scott Cinnamond
 * @since 1.0.9
 */
public class Qualifiers {
  private Map<String, Pair<PlasmaDataObject, PlasmaProperty>> map;
  private static byte[] delim = org.apache.hadoop.hbase.util.Bytes.toBytes("|");

  public Qualifiers() {
    super();
    this.map = new HashMap<>();
  }

  public void add(byte[] family, byte[] qualifier, PlasmaDataObject dataObject,
      PlasmaProperty property) {
    Pair<PlasmaDataObject, PlasmaProperty> pair = new Pair<PlasmaDataObject, PlasmaProperty>(
        dataObject, property);
    this.map.put(toKey(family, qualifier), pair);
  }

  public Pair<PlasmaDataObject, PlasmaProperty> get(byte[] family, byte[] qualifier) {
    return this.map.get(toKey(family, qualifier));
  }

  public static String toKey(byte[] family, byte[] qualifier) {
    return org.apache.hadoop.hbase.util.Bytes.toString(Bytes.concat(family, delim, qualifier));
  }

}
