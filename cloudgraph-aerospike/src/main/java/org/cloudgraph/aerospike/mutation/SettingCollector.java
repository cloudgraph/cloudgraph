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
package org.cloudgraph.aerospike.mutation;

import java.util.HashSet;
import java.util.List;

import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.core.NullValue;

import commonj.sdo.ChangeSummary.Setting;

/**
 * Collects old value (settings) for a given data type and property. As an
 * entire history of modifications is sent from the client, for each property
 * modification, collect a unique set of values to get a single collection of
 * original values set to the client. Use this set to compare against actual
 * values in a data store.
 * 
 * <p>
 * </p>
 * Note: the client edge collection can be the results from a slice query where
 * not all the collection results are returned.
 * 
 * <p>
 * <p>
 * Below is an example (in XML representation) of a modification where the
 * property was changed more than once as a result of multiple delete
 * operations.
 * 
 * <pre>
 * <ChangeSummary logging="true>
 *   <modified type="Actor" path="#/namespace:" id="5b33afbc-621f-4119-a029-ac262c4b4dea">
 *     <property name="photo" isSet="true" value="list[701866f5-780f-414e-bec5-b9e2512c7d0b, 2db24f1f-1164-4271-984a-9c3141e82b7d]" timestamp="1499121789352"</property>
 *     <property name="photo" isSet="true" value="list[2db24f1f-1164-4271-984a-9c3141e82b7d]" timestamp="1499121789352"</property><\modified>
 *   <deleted type="Photo" path="#/namespace:photo[0]" id="2db24f1f-1164-4271-984a-9c3141e82b7d">
 *     <property name="actor" isSet="true" value="list[5b33afbc-621f-4119-a029-ac262c4b4dea]" timestamp="1499121789352"</property>
 *     <property name="content" isSet="true" value="[B@a22c4d8" timestamp="1499121789352"</property><\deleted>
 *   <deleted type="Photo" path="#/namespace:photo[0]" id="701866f5-780f-414e-bec5-b9e2512c7d0b">
 *     <property name="actor" isSet="true" value="list[5b33afbc-621f-4119-a029-ac262c4b4dea]" timestamp="1499121789352"</property>
 *     <property name="content" isSet="true" value="[B@45cd7bc5" timestamp="1499121789351"</property><\deleted>
 * </ChangeSummary>
 * </pre>
 */
public class SettingCollector<T> {
  HashSet<T> collect(PlasmaProperty property, List<Setting> settings) {
    HashSet<T> result = new HashSet<T>();
    for (Setting setting : settings) {
      if (!setting.getProperty().equals(property))
        continue;
      Object oldValue = setting.getValue();
      if (!NullValue.class.isAssignableFrom(oldValue.getClass())) {
        if (List.class.isAssignableFrom(oldValue.getClass())) {
          List<T> oldValuesList = (List<T>) oldValue;
          for (T object : oldValuesList)
            result.add(object);
        } else {
          result.add((T) oldValue);
        }
      }
    }
    return result;
  }
}
