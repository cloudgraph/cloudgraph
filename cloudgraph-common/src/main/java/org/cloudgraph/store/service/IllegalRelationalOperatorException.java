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
package org.cloudgraph.store.service;

import org.plasma.query.model.RelationalOperatorName;

/**
 * @author Scott Cinnamond
 * @since 0.5
 */
public class IllegalRelationalOperatorException extends GraphServiceException {
  private static final long serialVersionUID = 1L;

  public IllegalRelationalOperatorException(RelationalOperatorName operator, int fieldSeqNum,
      String fieldPath) {
    super(
        "relational operator ("
            + toUserString(operator)
            + ") not allowed for user "
            + "defined row key fields with a hash algorithm applied - see configured hash settings for "
            + "user defined field (" + fieldSeqNum + ") with path '" + fieldPath + "'");
  }

  public IllegalRelationalOperatorException(String message) {
    super(message);
  }

  public IllegalRelationalOperatorException(Throwable t) {
    super(t);
  }

  private static String toUserString(RelationalOperatorName operator) {
    switch (operator) {
    case EQUALS:
      return "equals";
    case NOT_EQUALS:
      return "not equals";
    case GREATER_THAN:
      return "greater than";
    case GREATER_THAN_EQUALS:
      return "greater than equals";
    case LESS_THAN:
      return "less than";
    case LESS_THAN_EQUALS:
      return "less than equals";
    default:
      return operator.name();
    }
  }

}