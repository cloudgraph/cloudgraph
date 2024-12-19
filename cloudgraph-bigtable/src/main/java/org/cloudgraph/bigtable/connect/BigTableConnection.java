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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.cloudgraph.core.Connection;
import org.cloudgraph.core.client.TableName;
import org.cloudgraph.hbase.client.HBaseAdmin;
import org.cloudgraph.hbase.client.HBaseBufferedMutator;
import org.cloudgraph.hbase.client.HBaseRegionLocator;
import org.cloudgraph.hbase.client.HBaseTable;
import org.cloudgraph.store.mapping.StoreMappingProp;
import org.cloudgraph.store.service.GraphServiceException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Pooled BigTable connection wrapper which complies with Apache pool semantics
 * and maintains a cache or table handles for each connection.
 * 
 * <p>
 * For some HBase implementations, e.g. MAPR, the management of connections and
 * table handles is super critical, as the API is extremely performance and
 * resource costly at scale.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 2.0.1
 */
public class BigTableConnection implements Connection {
  private static Log log = LogFactory.getLog(BigTableConnection.class);
  private org.apache.hadoop.hbase.client.Connection con;
  private ObjectPool<Connection> pool;
  private Configuration config;
  private LoadingCache<TableName, Table> tableCache;

  public BigTableConnection(org.apache.hadoop.hbase.client.Connection conection,
      ObjectPool<Connection> pool, Configuration config) {
    super();
    this.con = conection;
    this.pool = pool;
    this.config = config;
    final int cacheMax = StoreMappingProp.getConnectionTablecacheSizeMax();
    final int cacheTimeout = StoreMappingProp.getConnectionTablecacheTimeoutSeconds();
    Map<String, String> propsMap = StoreMappingProp.getConnectionTableConfigProperties();
    Iterator<String> keys = propsMap.keySet().iterator();
    while (keys.hasNext()) {
      String key = keys.next();
      String value = propsMap.get(key);
      this.config.set(key, value);
    }

    this.tableCache = CacheBuilder.newBuilder().maximumSize(cacheMax)
        .expireAfterAccess(cacheTimeout, TimeUnit.SECONDS)
        .removalListener(new RemovalListener<TableName, Table>() {
          @Override
          public void onRemoval(RemovalNotification<TableName, Table> event) {
            try {
              event.getValue().close();
              if (log.isDebugEnabled())
                log.debug("closed evicted table " + this + " " + event.getKey());
            } catch (IOException e) {
              log.warn(e.getMessage(), e);
            }
          }
        }).build(new CacheLoader<TableName, Table>() {
          @Override
          public Table load(TableName tableName) throws Exception {
            if (log.isDebugEnabled())
              log.debug("loading table " + this + " " + tableName);
            org.apache.hadoop.hbase.TableName hbaseTableName = org.apache.hadoop.hbase.TableName
                .valueOf(tableName.getNamespace(), tableName.getTableName());
            return con.getTable(hbaseTableName);
          }
        });
    if (log.isDebugEnabled())
      log.debug("created " + this + " pool active/idle " + pool.getNumActive() + "/"
          + pool.getNumIdle() + " tableCacheMax/tableCacheTimeout: " + cacheMax + "/"
          + cacheTimeout);
  }

  @Override
  public void close() throws IOException {
    if (log.isDebugEnabled())
      log.debug("closing wrapped connection, " + this.con);
    try {
      this.pool.returnObject(this);
    } catch (Exception e) {
      throw new GraphServiceException(e);
    }
  }

  @Override
  public void destroy() throws IOException {
    this.tableCache.invalidateAll();
    this.tableCache.cleanUp();
    // above should evict and close any tables but just to be sure
    for (Table table : this.tableCache.asMap().values())
      table.close();
    if (log.isDebugEnabled())
      log.debug("destroyed " + this + " pool active/idle " + pool.getNumActive() + "/"
          + pool.getNumIdle());
  }

  @Override
  public boolean isClosed() {
    return con.isClosed();
  }

  @Override
  public void abort(String why, Throwable e) {
    con.abort(why, e);
  }

  @Override
  public boolean isAborted() {
    return con.isAborted();
  }

  public Configuration getConfiguration() {
    return con.getConfiguration();
  }

  @Override
  public boolean tableExists(TableName tableName) throws IOException {
    boolean exists = false;
    Table table = this.tableCache.getIfPresent(tableName);
    if (table != null) {
      exists = true;
    } else {
      org.apache.hadoop.hbase.TableName hbaseTableName = org.apache.hadoop.hbase.TableName.valueOf(
          tableName.getNamespace(), tableName.getTableName());
      // expensive
      // exists = con.getAdmin().tableExists(hbaseTableName);
      try {
        table = con.getTable(hbaseTableName);
      } catch (IOException e) {
        log.debug("table not found: " + hbaseTableName);
      }
      exists = table != null;

      if (exists) {
        // try {
        this.tableCache.put(tableName, table);
        // } catch (ExecutionException e) {
        // log.error(e.getMessage(), e);
        // }
      }
    }
    return exists;
  }

  @Override
  public org.cloudgraph.core.client.Table getTable(TableName tableName) throws IOException {
    Table result = null;
    try {
      result = this.tableCache.get(tableName);
    } catch (ExecutionException e) {
      log.error(e.getMessage(), e);
    }
    if (result != null)
      return new HBaseTable(result);
    else
      return null;
  }

  @Override
  public org.cloudgraph.core.client.Table getTable(TableName tableName, ExecutorService pool)
      throws IOException {
    Table result = null;
    try {
      result = this.tableCache.get(tableName);
    } catch (ExecutionException e) {
      log.error(e.getMessage(), e);
    }
    if (result != null)
      return new HBaseTable(result);
    else
      return null;

  }

  @Override
  public org.cloudgraph.core.client.BufferedMutator getBufferedMutator(TableName tableName)
      throws IOException {
    BufferedMutator result = con.getBufferedMutator(org.apache.hadoop.hbase.TableName.valueOf(
        tableName.getNamespace(), tableName.getTableName()));
    if (result != null)
      return new HBaseBufferedMutator(result);
    return null;
  }

  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    return con.getBufferedMutator(params);
  }

  @Override
  public org.cloudgraph.core.client.RegionLocator getRegionLocator(TableName tableName)
      throws IOException {

    RegionLocator result = con.getRegionLocator(org.apache.hadoop.hbase.TableName.valueOf(
        tableName.getNamespace(), tableName.getTableName()));
    if (result != null)
      return new HBaseRegionLocator(result);
    return null;
  }

  @Override
  public org.cloudgraph.core.client.Admin getAdmin() throws IOException {
    return new HBaseAdmin(con.getAdmin());
  }

}
