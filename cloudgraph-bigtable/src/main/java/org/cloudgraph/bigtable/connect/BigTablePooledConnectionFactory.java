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
package org.cloudgraph.bigtable.connect;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.cloudgraph.core.Connection;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

/**
 * Connection factory which creates pooled connections.
 * 
 * @author Scott Cinnamond
 * @since 2.0.0
 */
public class BigTablePooledConnectionFactory extends BasePooledObjectFactory<Connection> {
  private static Log log = LogFactory.getLog(BigTablePooledConnectionFactory.class);

  private Configuration config;
  private ObjectPool<Connection> pool;

  public BigTablePooledConnectionFactory(Configuration config) {
    super();
    this.config = config;
  }

  public void setPool(ObjectPool<Connection> pool) {
    this.pool = pool;
  }

  @Override
  public Connection create() throws Exception {
    String project = this.config.get(BigTableConnectionManager.CONNECTION_FACTORY_PROJECT);
    String instance = this.config.get(BigTableConnectionManager.CONNECTION_FACTORY_INSTANCE);
    org.apache.hadoop.hbase.client.Connection con = BigtableConfiguration
        .connect(project, instance);

    if (log.isDebugEnabled())
      log.debug("created new bigtable connection  " + con + " for project/instance: " + project
          + "/" + instance);
    return new BigTableConnection(con, this.pool, this.config);
  }

  @Override
  public PooledObject<Connection> wrap(Connection con) {
    return new DefaultPooledObject<Connection>(con);
  }

  @Override
  public void destroyObject(PooledObject<Connection> p) throws Exception {
    if (log.isDebugEnabled())
      log.debug("destroying connection" + p.getObject());
    p.getObject().destroy();
    super.destroyObject(p);

  }

  @Override
  public boolean validateObject(PooledObject<Connection> p) {
    if (log.isDebugEnabled())
      log.debug("validating connection" + p.getObject());
    return super.validateObject(p);
  }

  @Override
  public void activateObject(PooledObject<Connection> p) throws Exception {
    if (log.isDebugEnabled())
      log.debug("activate connection" + p.getObject());
    super.activateObject(p);
  }

  @Override
  public void passivateObject(PooledObject<Connection> p) throws Exception {
    if (log.isDebugEnabled())
      log.debug("passivate connection" + p.getObject());
    super.passivateObject(p);
  }
}
