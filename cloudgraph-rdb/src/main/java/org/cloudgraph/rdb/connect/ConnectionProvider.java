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
package org.cloudgraph.rdb.connect;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Supplies connections from a source pool or data source. Connections should
 * always be closed after use.
 */
public interface ConnectionProvider {

  /**
   * Returns a connection from an underlying pool or data source. Connections
   * should always be closed after use.
   * 
   * @return the connection
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException;
}
