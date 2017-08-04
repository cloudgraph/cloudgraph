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
package org.cloudgraph.hbase.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.state.StateMarshalingContext;

public class SimpleServiceContext implements ServiceContext {
  private static Log log = LogFactory.getLog(SimpleServiceContext.class);
  private StateMarshalingContext marshallingContext;

  @SuppressWarnings("unused")
  private SimpleServiceContext() {
  }

  public SimpleServiceContext(StateMarshalingContext marshallingContext) {
    this.marshallingContext = marshallingContext;
  }

  @Override
  public StateMarshalingContext getMarshallingContext() {
    return this.marshallingContext;
  }

  @Override
  public void close() {
  }
}
