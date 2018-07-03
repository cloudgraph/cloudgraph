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
package org.cloudgraph.hbase.results;

import org.plasma.query.model.Path;
import org.plasma.sdo.access.provider.common.DataGraphComparator;

import commonj.sdo.Property;

public class ResultsComparator extends DataGraphComparator {

  public boolean contains(Property property, Path path) {
    for (PathInfo info : this.paths) {
      if (info.property.getName().equals(property.getName())) {
        if (path != null && path.size() > 0) {
          if (info.property.getPath() != null && info.property.getPath().size() > 0)
            if (info.property.getPath().equals(path))
              return true;
        } else {
          return true;
        }
      }
    }
    return false;
  }
}
