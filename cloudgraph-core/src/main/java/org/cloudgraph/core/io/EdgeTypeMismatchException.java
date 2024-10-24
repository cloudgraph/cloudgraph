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
package org.cloudgraph.core.io;

import org.cloudgraph.core.io.OperationException;

/**
 * Thrown where an attempt is made to add a data object to a new or existing
 * edge where the edge base type or subtype have already been established and
 * the given type does not match.
 * 
 * @author Scott Cinnamond
 * @since 1.0.0
 */
public class EdgeTypeMismatchException extends OperationException {

  private static final long serialVersionUID = 1L;

  public EdgeTypeMismatchException() {
    super();
  }

  public EdgeTypeMismatchException(String msg) {
    super(msg);
  }

  public EdgeTypeMismatchException(Throwable t) {
    super(t);
  }
}
