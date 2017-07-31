/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.hbase.connect;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;

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

  private Configuration config;
  private ObjectPool<Connection> pool;

  public PooledConnectionFactory(Configuration config) {
    super();
    this.config = config;
  }

  public void setPool(ObjectPool<Connection> pool) {
    this.pool = pool;
  }

  @Override
  public Connection create() throws Exception {
    org.apache.hadoop.hbase.client.Connection con = ConnectionFactory.createConnection(config);
    if (log.isDebugEnabled())
      log.debug("creating new hbase connection" + con);
    return new Connection(con, this.pool);
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
