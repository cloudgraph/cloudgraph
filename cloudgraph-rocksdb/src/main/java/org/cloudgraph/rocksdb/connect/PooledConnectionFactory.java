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
package org.cloudgraph.rocksdb.connect;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.cloudgraph.core.Connection;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.store.mapping.StoreMappingContext;

/**
 * Connection factory which creates pooled connections.
 * <p>
 * </p>
 * The new HBase Client API changes removed the existing connection pool
 * implementation and placed the responsibility of managing the lifecycle of
 * connections on the caller. A pool is necessary as creating connections to the
 * cluster via. zookeeper is fairly expensive.
 * 
 * @author Scott Cinnamond
 * @since 0.6.3
 */
public class PooledConnectionFactory extends BasePooledObjectFactory<Connection> {
  private static Log log = LogFactory.getLog(PooledConnectionFactory.class);

  private ObjectPool<Connection> pool;
  private ServiceContext serviceContext;

  public PooledConnectionFactory(ServiceContext serviceContext) {
    super();
    this.serviceContext = serviceContext;
  }

  public void setPool(ObjectPool<Connection> pool) {
    this.pool = pool;
  }

  @Override
  public Connection create() throws Exception {
    if (log.isDebugEnabled())
      log.debug("creating new hbase connection");
    return new RocksDBConnection(this.pool, this.serviceContext);
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
