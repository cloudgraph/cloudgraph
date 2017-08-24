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
package org.cloudgraph.hbase.scan;

import org.cloudgraph.config.UserDefinedRowKeyFieldConfig;
import org.plasma.query.model.LogicalOperatorName;
import org.plasma.query.model.RelationalOperatorName;

/**
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class ImbalancedOperatorMappingException extends ScanException {
  private static final long serialVersionUID = 1L;

  public ImbalancedOperatorMappingException() {
    super();
  }

  public ImbalancedOperatorMappingException(String msg) {
    super(msg);
  }

  public ImbalancedOperatorMappingException(Throwable t) {
    super(t);
  }

  public ImbalancedOperatorMappingException(RelationalOperatorName left,
      LogicalOperatorName operator, RelationalOperatorName right, UserDefinedRowKeyFieldConfig field) {
    super("relational operator '" + left + "' linked through logical operator '" + operator
        + "' to relational operator '" + right + "' for row key field property, "
        + field.getEndpointProperty().getContainingType().toString() + "."
        + field.getEndpointProperty().getName());
  }

}
