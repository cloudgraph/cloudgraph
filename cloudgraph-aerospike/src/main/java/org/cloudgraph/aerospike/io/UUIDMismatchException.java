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
package org.cloudgraph.aerospike.io;

/**
 * For where 2 UUID's or UUID strings from 2 different contexts are expected to
 * match.
 * <p>
 * </p>
 * Thrown where e.g. a UUID which is queried from a data store is expected to
 * match a UUID within a data object or change summary passed to a service.
 * 
 * @author Scott Cinnamond
 * @since 0.6.4
 */
public class UUIDMismatchException extends OperationException {

  private static final long serialVersionUID = 1L;

  public UUIDMismatchException() {
    super();
  }

  public UUIDMismatchException(String msg) {
    super(msg);
  }

  public UUIDMismatchException(Throwable t) {
    super(t);
  }
}
