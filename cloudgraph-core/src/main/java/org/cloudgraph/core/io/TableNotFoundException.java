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

/**
 * Thrown when a referenced table does not exist.
 * 
 * @author Scott Cinnamond
 * @since 2.0.6
 */
public class TableNotFoundException extends OperationException {

  private static final long serialVersionUID = 1L;

  public TableNotFoundException() {
    super();
  }

  public TableNotFoundException(String msg) {
    super(msg);
  }

  public TableNotFoundException(Throwable t) {
    super(t);
  }
}
