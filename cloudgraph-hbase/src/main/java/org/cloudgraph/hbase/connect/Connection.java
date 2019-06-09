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
package org.cloudgraph.hbase.connect;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.cloudgraph.store.mapping.StoreMappingProp;
import org.cloudgraph.store.service.GraphServiceException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Connection wrapper.
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
public class Connection {
  private static Log log = LogFactory.getLog(Connection.class);
  private org.apache.hadoop.hbase.client.Connection con;
  private ObjectPool<Connection> pool;
  private Configuration config;
  private LoadingCache<TableName, Table> tableCache;

  public Connection(org.apache.hadoop.hbase.client.Connection conection,
      ObjectPool<Connection> pool, Configuration config) {
    super();
    this.con = conection;
    this.pool = pool;
    this.config = config;
    // FIXME: configure table cache
    final int cacheMax = StoreMappingProp.getHBaseConnectionTablecacheSizeMax();
    final int cacheTimeout = StoreMappingProp.getHBaseConnectionTablecacheTimeoutSeconds();
    
    this.tableCache = CacheBuilder.newBuilder().maximumSize(cacheMax)
        .expireAfterAccess(cacheTimeout, TimeUnit.SECONDS).build(new CacheLoader<TableName, Table>() {
          @Override
          public Table load(TableName tableName) throws Exception {
            if (log.isDebugEnabled())
              log.debug("loading table " + this + " " + tableName);
            return con.getTable(tableName);
          }
        });
    if (log.isDebugEnabled())
      log.debug("created " + this + " pool active/idle " + pool.getNumActive() + "/"
          + pool.getNumIdle());
  }

  public void close() throws IOException {
    if (log.isDebugEnabled())
      log.debug("closing wrapped connection, " + this.con);
    try {
      this.pool.returnObject(this);
    } catch (Exception e) {
      throw new GraphServiceException(e);
    }
  }

  public void destroy() throws IOException {
    for (Table table : this.tableCache.asMap().values())
      table.close();
    if (log.isDebugEnabled())
      log.debug("destroyed " + this + " pool active/idle " + pool.getNumActive() + "/"
          + pool.getNumIdle());
  }

  public boolean isClosed() {
    return con.isClosed();
  }

  public void abort(String why, Throwable e) {
    con.abort(why, e);
  }

  public boolean isAborted() {
    return con.isAborted();
  }

  public Configuration getConfiguration() {
    return con.getConfiguration();
  }

  public boolean tableExists(TableName tableName) throws IOException {
    boolean exists = false;
    Table table = this.tableCache.getIfPresent(tableName);
    if (table != null) {
      exists = true;
    } else {
      exists = getAdmin().tableExists(tableName);
      if (exists) {
        try {
          this.tableCache.get(tableName);
        } catch (ExecutionException e) {
          log.error(e.getMessage(), e);
        }
      }
    }
    return exists;
  }

  public Table getTable(TableName tableName) throws IOException {
    Table result = null;
    try {
      result = this.tableCache.get(tableName);
    } catch (ExecutionException e) {
      log.error(e.getMessage(), e);
    }
    return result;
  }

  public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
    Table result = null;
    try {
      result = this.tableCache.get(tableName);
    } catch (ExecutionException e) {
      log.error(e.getMessage(), e);
    }
    return result;
  }

  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return con.getBufferedMutator(tableName);
  }

  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    return con.getBufferedMutator(params);
  }

  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return con.getRegionLocator(tableName);
  }

  public Admin getAdmin() throws IOException {
    return con.getAdmin();
  }

}
