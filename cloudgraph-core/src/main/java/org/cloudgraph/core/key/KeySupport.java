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
package org.cloudgraph.core.key;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.store.key.KeyValue;
import org.cloudgraph.store.mapping.DataRowKeyFieldMapping;
import org.plasma.sdo.PlasmaProperty;

/**
 * Delegate class supporting composite key generation.
 * 
 * @author Scott Cinnamond
 * @since 0.5
 */
public class KeySupport {
  private static final Log log = LogFactory.getLog(CompositeRowKeyFactory.class);

  public KeyValue findKeyValue(DataRowKeyFieldMapping fieldConfig, List<KeyValue> pairs) {

    PlasmaProperty fieldProperty = fieldConfig.getEndpointProperty();

    for (KeyValue keyValue : pairs) {
      if (keyValue.getProp().equals(fieldProperty)) {
        if (fieldConfig.getPropertyPath() != null && keyValue.getPropertyPath() != null) {
          if (keyValue.getPropertyPath().equals(fieldConfig.getPropertyPath())) {
            return keyValue;
          }
        }
      }
    }
    return null;
  }

}
