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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
//import org.apache.hadoop.hbase.client.Admin;
import org.cloudgraph.core.Connection;
import org.cloudgraph.core.ConnectionConstants;
import org.cloudgraph.core.ConnectionManager;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.Admin;
import org.cloudgraph.hbase.client.HBaseAdmin;
import org.cloudgraph.hbase.client.HBaseTableName;
import org.cloudgraph.hbase.service.CloudGraphContext;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.service.GraphServiceException;

/**
 * Manages HBase table pool and table interface access.
 * 
 * @see CloudGraphContext
 * @see TableMapping
 * @author Scott Cinnamond
 * @since 0.5
 */
public class HBaseConnectionManager implements ConnectionManager, ConnectionConstants {

  private GenericObjectPool<Connection> pool;

  private static final Log log = LogFactory.getLog(HBaseConnectionManager.class);

  private static volatile ConnectionManager instance;
  private Configuration config;

  private HBaseConnectionManager() {
    this.config = CloudGraphContext.instance().getConfig();

    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

    poolConfig.setMaxTotal(this.config.getInt(CONNECTION_POOL_MAX_TOTAL,
        GenericObjectPoolConfig.DEFAULT_MAX_TOTAL));
    if (this.config.get(CONNECTION_POOL_MAX_SIZE) != null)
      poolConfig.setMaxTotal(this.config.getInt(CONNECTION_POOL_MAX_SIZE,
          GenericObjectPoolConfig.DEFAULT_MAX_TOTAL));

    poolConfig.setMaxIdle(this.config.getInt(CONNECTION_POOL_MAX_IDLE,
        GenericObjectPoolConfig.DEFAULT_MAX_IDLE));
    poolConfig.setMinIdle(this.config.getInt(CONNECTION_POOL_MIN_IDLE,
        GenericObjectPoolConfig.DEFAULT_MIN_IDLE));
    if (this.config.get(CONNECTION_POOL_MIN_SIZE) != null)
      poolConfig.setMinIdle(this.config.getInt(CONNECTION_POOL_MIN_SIZE,
          GenericObjectPoolConfig.DEFAULT_MIN_IDLE));

    poolConfig.setLifo(this.config.getBoolean(CONNECTION_POOL_LIFO,
        GenericObjectPoolConfig.DEFAULT_LIFO));

    poolConfig.setMaxWaitMillis(this.config.getLong(CONNECTION_POOL_MAX_WAIT_MILLIS,
        GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS));

    // eviction
    poolConfig.setTimeBetweenEvictionRunsMillis(this.config.getLong(
        CONNECTION_POOL_TIME_BETWEEN_EVICTION_RUNS_MILLIS,
        GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS));
    poolConfig.setEvictionPolicyClassName(this.config.get(
        CONNECTION_POOL_EVICTION_POLICY_CLASS_NAME,
        GenericObjectPoolConfig.DEFAULT_EVICTION_POLICY_CLASS_NAME));
    poolConfig.setMinEvictableIdleTimeMillis(this.config.getLong(
        CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS,
        GenericObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS));
    poolConfig.setSoftMinEvictableIdleTimeMillis(this.config.getLong(
        CONNECTION_POOL_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS,
        GenericObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS));
    poolConfig.setNumTestsPerEvictionRun(this.config.getInt(
        CONNECTION_POOL_NUM_TESTS_PER_EVICTION_RUN,
        GenericObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN));

    poolConfig.setTestOnCreate(this.config.getBoolean(CONNECTION_POOL_TEST_ON_CREATE,
        GenericObjectPoolConfig.DEFAULT_TEST_ON_CREATE));
    poolConfig.setTestOnBorrow(this.config.getBoolean(CONNECTION_POOL_TEST_ON_BORROW,
        GenericObjectPoolConfig.DEFAULT_TEST_ON_BORROW));
    poolConfig.setTestOnReturn(this.config.getBoolean(CONNECTION_POOL_TEST_ON_RETURN,
        GenericObjectPoolConfig.DEFAULT_TEST_ON_RETURN));
    poolConfig.setTestWhileIdle(this.config.getBoolean(CONNECTION_POOL_TEST_WHILE_IDLE,
        GenericObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE));
    poolConfig.setBlockWhenExhausted(this.config.getBoolean(CONNECTION_POOL_BLOCK_WHEN_EXHAUSTED,
        GenericObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED));
    poolConfig.setJmxEnabled(this.config.getBoolean(CONNECTION_POOL_JMX_ENABLED, false));
    poolConfig.setJmxNameBase(this.config.get(CONNECTION_POOL_JMX_NAME_BASE,
        GenericObjectPoolConfig.DEFAULT_JMX_NAME_BASE));
    poolConfig.setJmxNamePrefix(this.config.get(CONNECTION_POOL_JMX_NAME_PREFIX,
        GenericObjectPoolConfig.DEFAULT_JMX_NAME_PREFIX));

    HBasePooledConnectionFactory factory = new HBasePooledConnectionFactory(this.config);
    this.pool = new GenericObjectPool<Connection>(factory, poolConfig);
    factory.setPool(pool);

    log.info("created connection pool[ " + "\n\tMaxTotal:\t\t" + poolConfig.getMaxTotal()
        + "\n\tMinIdle:\t\t" + poolConfig.getMinIdle() + "\n\tMaxIdle:\t\t"
        + poolConfig.getMaxIdle() + "\n\tLifo:\t\t" + poolConfig.getLifo()
        + "\n\tMaxWaitMillis:\t\t" + poolConfig.getMaxWaitMillis()
        + "\n\tTimeBetweenEvictionRunsMillis:\t\t" + poolConfig.getTimeBetweenEvictionRunsMillis()
        + "\n\tEvictionPolicyClassName:\t\t" + poolConfig.getEvictionPolicyClassName()
        + "\n\tMinEvictableIdleTimeMillis:\t\t" + poolConfig.getMinEvictableIdleTimeMillis()
        + "\n\tSoftMinEvictableIdleTimeMillis:\t\t"
        + poolConfig.getSoftMinEvictableIdleTimeMillis() + "\n\tNumTestsPerEvictionRun:\t\t"
        + poolConfig.getNumTestsPerEvictionRun() + "\n...]");
  }

  public static ConnectionManager instance() {
    if (instance == null)
      initInstance();
    return instance;
  }

  private static synchronized void initInstance() {
    if (instance == null)
      instance = new HBaseConnectionManager();
  }

  protected void finalize() {
    log.debug("Finalizing ConnectionManager");
    try {
      super.finalize();
    } catch (Throwable ex) {
      log.error("ConnectionManager finalize failed to disconnect: ", ex);
    }
  }

  @Override
  public Connection getConnection() {
    try {
      return this.pool.borrowObject();
    } catch (Exception e) {
      throw new GraphServiceException(e);
    } finally {
    }
  }

  @Override
  public void createTable(org.cloudgraph.core.Connection connection,
      org.cloudgraph.core.client.TableName name, ServiceContext serviceContext) {

    org.apache.hadoop.hbase.client.Admin hbaseAdmin = null;
    try {
      hbaseAdmin = HBaseAdmin.class.cast(connection.getAdmin()).getAdmin();

      TableMapping tableConfig = StoreMapping.getInstance().getTableByQualifiedLogicalName(
          name.getQualifiedLogicalName(serviceContext.getStoreMapping()),
          serviceContext.getStoreMapping());

      HBaseTableName hbaseTableName = (HBaseTableName) name;

      HTableDescriptor tableDesc = new HTableDescriptor(hbaseTableName.get());
      HColumnDescriptor fam1 = new HColumnDescriptor(tableConfig.getDataColumnFamilyName()
          .getBytes());
      tableDesc.addFamily(fam1);
      try {
        hbaseAdmin.createTable(tableDesc);
      } catch (NamespaceNotFoundException nnf) {
        NamespaceDescriptor namespace = NamespaceDescriptor
            .create(hbaseTableName.get().getNamespaceAsString())
            .addConfiguration("Description", "cloudgraph generated namespace").build();
        hbaseAdmin.createNamespace(namespace);
        hbaseAdmin.createTable(tableDesc);
      }
    } catch (MasterNotRunningException e1) {
      throw new GraphServiceException(e1);
    } catch (ZooKeeperConnectionException e1) {
      throw new GraphServiceException(e1);
    } catch (IOException e) {
      throw new GraphServiceException(e);
    } finally {
      if (hbaseAdmin != null)
        try {
          hbaseAdmin.close();
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
    }
  }

  @Override
  public void deleteTable(org.cloudgraph.core.Connection connection,
      org.cloudgraph.core.client.TableName name) {

    org.apache.hadoop.hbase.client.Admin hbaseAdmin = null;
    try {
      hbaseAdmin = HBaseAdmin.class.cast(connection.getAdmin()).getAdmin();
      TableName hbaseTableName = TableName.valueOf(name.getNamespace(), name.getTableName());
      hbaseAdmin.disableTable(hbaseTableName);
      hbaseAdmin.deleteTable(hbaseTableName);
    } catch (MasterNotRunningException e1) {
      throw new GraphServiceException(e1);
    } catch (ZooKeeperConnectionException e1) {
      throw new GraphServiceException(e1);
    } catch (IOException e) {
      throw new GraphServiceException(e);
    } finally {
      if (hbaseAdmin != null)
        try {
          hbaseAdmin.close();
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
    }
  }

}
