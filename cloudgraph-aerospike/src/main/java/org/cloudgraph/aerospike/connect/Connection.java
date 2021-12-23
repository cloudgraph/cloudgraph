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
package org.cloudgraph.aerospike.connect;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.ObjectPool;
import org.cloudgraph.aerospike.ext.Table;
import org.cloudgraph.aerospike.ext.TableName;
import org.cloudgraph.aerospike.service.CloudGraphContext;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.StoreMappingProp;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.service.GraphServiceException;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Pooled HBase connection wrapper which complies with Apache pool semantics and
 * maintains a cache or table handles for each connection. Depending in the
 * complexity of a particular application, there may be many tables needed, for
 * example some tables for actual data and some for metadata or other
 * application data. Caching table API handles greatly reduces round trips and
 * resource costs for some HBase implementations.
 * <p>
 * The new HBase 1.x Client API changes removed the existing connection pool
 * implementation and placed the responsibility of managing the lifecycle of
 * connections on the caller. A pool is necessary as creating connections to the
 * cluster via. zookeeper is fairly expensive.
 * </p>
 * 
 * <p>
 * For some HBase implementations, e.g. MAPR, the management of connections and
 * table handles is super critical, as the API is extremely performance and
 * resource costly at scale.
 * </p>
 * 
 * @author Scott Cinnamond
 * @since 0.6.3
 */
public class Connection {
  private static Log log = LogFactory.getLog(Connection.class);
  private ObjectPool<Connection> pool;
  private LoadingCache<TableName, Table> tableCache;
  private AerospikeClient connection;
  private StoreMappingContext mappingContext;

  public Connection(ObjectPool<Connection> pool, StoreMappingContext mappingContext) {
    super();
    this.pool = pool;
    this.mappingContext = mappingContext;
    ClientPolicy policy = new ClientPolicy();
    policy.authMode = AuthMode.EXTERNAL_INSECURE;
    // this.connection = new AerospikeClient(policy, "192.168.1.134", 3000);
    // new AerospikeClient(CloudGraphContext.instance().getConfig(),
    Host[] hosts = CloudGraphContext.instance().getHosts();
    this.connection = new AerospikeClient(policy, hosts);
    final int cacheMax = StoreMappingProp.getHBaseConnectionTablecacheSizeMax();
    final int cacheTimeout = StoreMappingProp.getHBaseConnectionTablecacheTimeoutSeconds();
    Map<String, String> propsMap = StoreMappingProp.getHBaseConnectionTableConfigProperties();
    Iterator<String> keys = propsMap.keySet().iterator();
    while (keys.hasNext()) {
      String key = keys.next();
      String value = propsMap.get(key);
    }

    this.tableCache = CacheBuilder.newBuilder().maximumSize(cacheMax)
        .expireAfterAccess(cacheTimeout, TimeUnit.SECONDS)
        .removalListener(new RemovalListener<TableName, Table>() {
          @Override
          public void onRemoval(RemovalNotification<TableName, Table> event) {
          }
        }).build(new CacheLoader<TableName, Table>() {
          @Override
          public Table load(TableName tableName) throws Exception {
            if (log.isDebugEnabled())
              log.debug("loading table " + this + " " + tableName.getNamespace() + "."
                  + tableName.getTableName());
            // TableMapping tableConfig =
            // StoreMapping.getInstance().getTableByQualifiedLogicalName(
            // name.getNamespaceAsString(), qualifiedLogicalName,
            // mappingContext);
            TableMapping tableConfig = StoreMapping.getInstance().getTableByQualifiedLogicalName(
                tableName.getNamespace(), tableName.getTableName(), mappingContext);
            // QName qname = new QName(tableName.getTableName(),
            // tableName.getNamespace());
            // TableMapping tableConfig =
            // StoreMapping.getInstance().getTable(qname,
            // mappingContext);
            return new Table(tableName, connection, tableConfig, mappingContext);
          }
        });
    if (log.isDebugEnabled())
      log.debug("created " + this + " pool active/idle " + pool.getNumActive() + "/"
          + pool.getNumIdle());
  }

  public void close() throws IOException {
    try {
      this.pool.returnObject(this);
    } catch (Exception e) {
      throw new GraphServiceException(e);
    }
  }

  public void destroy() throws IOException {
    this.tableCache.invalidateAll();
    this.tableCache.cleanUp();
    if (log.isDebugEnabled())
      log.debug("destroyed " + this + " pool active/idle " + pool.getNumActive() + "/"
          + pool.getNumIdle());
  }

  public boolean isClosed() {
    return false;
  }

  public void abort(String why, Throwable e) {
  }

  public boolean isAborted() {
    return false;
  }

  public boolean tableExists(TableName tableName) throws IOException {
    boolean exists = true;
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

}
