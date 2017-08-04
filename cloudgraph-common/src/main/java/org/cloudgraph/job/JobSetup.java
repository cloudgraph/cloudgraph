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
package org.cloudgraph.job;

import org.plasma.query.Query;
import org.plasma.query.model.From;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.helper.PlasmaTypeHelper;

/**
 * A parallel compute job.
 * 
 * @author Scott Cinnamond
 * @since 0.6.3
 */
public abstract class JobSetup {

  protected static PlasmaType getRootType(Query query) {
    From from = query.getModel().getFromClause();
    if (from.getEntity() == null)
      throw new IllegalArgumentException("given query has no root type and/or URI");
    if (from.getEntity().getName() == null || from.getEntity().getNamespaceURI() == null)
      throw new IllegalArgumentException("given query has no root type and/or URI");
    PlasmaType type = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(from.getEntity()
        .getNamespaceURI(), from.getEntity().getName());
    return type;
  }

}
