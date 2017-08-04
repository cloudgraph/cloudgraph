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

import java.util.HashSet;

import org.cloudgraph.common.CloudGraphConstants;
import org.plasma.sdo.PlasmaDataGraphVisitor;
import org.plasma.sdo.core.CoreNode;

import commonj.sdo.DataObject;

public class GraphMetricVisitor implements PlasmaDataGraphVisitor {

  private long count = 0;
  private long depth = 0;
  private HashSet<String> threadNames = new HashSet<String>();

  @Override
  public void visit(DataObject target, DataObject source, String sourcePropertyName, int level) {
    count++;
    if (level > depth)
      depth = level;

    CoreNode node = (CoreNode) target;
    String thread = (String) node.getValueObject().get(CloudGraphConstants.GRAPH_NODE_THREAD_NAME);
    if (thread != null)
      this.threadNames.add(thread);

  }

  public long getCount() {
    return count;
  }

  public long getDepth() {
    return depth;
  }

  public long getThreadCount() {
    return threadNames.size();
  }
}
