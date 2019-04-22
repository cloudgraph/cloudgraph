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
import org.apache.hadoop.hbase.client.Admin;
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
public class HBaseConnectionManager {

  /**
   * Synonym for min idle. The property name for the {@code minIdle}
   * configuration attribute for connection pools.
   */
  public static final String CONNECTION_POOL_MIN_SIZE = "org.plasma.sdo.access.provider.hbase.ConnectionPoolMinSize";

  /**
   * Synonym for max total. The property name for the {@code maxTotal}
   * configuration attribute for connection pools.
   */
  public static final String CONNECTION_POOL_MAX_SIZE = "org.plasma.sdo.access.provider.hbase.ConnectionPoolMaxSize";

  /**
   * The property name for the {@code maxTotal} configuration attribute for
   * connection pools.
   *
   *
   * @see GenericObjectPool#setMaxTotal(int)
   */
  public static final String CONNECTION_POOL_MAX_TOTAL = "org.plasma.sdo.access.provider.hbase.ConnectionPoolMaxTotal";

  /**
   * The property name for the {@code maxIdle} configuration attribute for
   * connection pools.
   *
   * @see GenericObjectPool#setMaxIdle(int)
   */
  public static final String CONNECTION_POOL_MAX_IDLE = "org.plasma.sdo.access.provider.hbase.ConnectionPoolMaxIdle";

  /**
   * The property name for the {@code minIdle} configuration attribute for
   * connection pools.
   *
   *
   * @see GenericObjectPool#setMinIdle(int)
   */
  public static final String CONNECTION_POOL_MIN_IDLE = "org.plasma.sdo.access.provider.hbase.ConnectionPoolMinIdle";

  /**
   * The property name for the {@code lifo} configuration attribute for pools
   * created with this configuration instance.
   *
   * @see GenericObjectPool#getLifo()
   * @see GenericKeyedObjectPool#getLifo()
   */
  public static final String CONNECTION_POOL_LIFO = "org.plasma.sdo.access.provider.hbase.ConnectionPoolLifo";

  /**
   * The property name for the {@code maxWait} configuration attribute for pools
   * created with this configuration instance.
   *
   * @see GenericObjectPool#getMaxWaitMillis()
   * @see GenericKeyedObjectPool#getMaxWaitMillis()
   */
  public static final String CONNECTION_POOL_MAX_WAIT_MILLIS = "org.plasma.sdo.access.provider.hbase.ConnectionPoolMaxWaitMillis";

  /**
   * The property name for the {@code minEvictableIdleTimeMillis} configuration
   * attribute for connection pools.
   *
   * @see GenericObjectPool#getMinEvictableIdleTimeMillis()
   * @see GenericKeyedObjectPool#getMinEvictableIdleTimeMillis()
   */
  public static final String CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS = "org.plasma.sdo.access.provider.hbase.ConnectionPoolMinEvictableIdleTimeMillis";

  /**
   * The property name for the {@code softMinEvictableIdleTimeMillis}
   * configuration attribute for pools created with this configuration instance.
   *
   * @see GenericObjectPool#getSoftMinEvictableIdleTimeMillis()
   * @see GenericKeyedObjectPool#getSoftMinEvictableIdleTimeMillis()
   */
  public static final String CONNECTION_POOL_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = "org.plasma.sdo.access.provider.hbase.ConnectionPoolSoftMinEvictableIdleTimeMillis";

  /**
   * The property name for the {@code numTestsPerEvictionRun} configuration
   * attribute for connection pools.
   *
   *
   * @see GenericObjectPool#getNumTestsPerEvictionRun()
   * @see GenericKeyedObjectPool#getNumTestsPerEvictionRun()
   */
  public static final String CONNECTION_POOL_NUM_TESTS_PER_EVICTION_RUN = "org.plasma.sdo.access.provider.hbase.ConnectionPoolNumTestsPerEvictionRun";

  /**
   * The property name for the {@code testOnCreate} configuration attribute for
   * connection pools.
   *
   *
   * @see GenericObjectPool#getTestOnCreate()
   * @see GenericKeyedObjectPool#getTestOnCreate()
   */
  public static final String CONNECTION_POOL_TEST_ON_CREATE = "org.plasma.sdo.access.provider.hbase.ConnectionPoolTestOnCreate";

  /**
   * The property name for the {@code testOnBorrow} configuration attribute for
   * connection pools.
   *
   * @see GenericObjectPool#getTestOnBorrow()
   * @see GenericKeyedObjectPool#getTestOnBorrow()
   */
  public static final String CONNECTION_POOL_TEST_ON_BORROW = "org.plasma.sdo.access.provider.hbase.ConnectionPoolTestOnBorrow";

  /**
   * The property name for the {@code testOnReturn} configuration attribute for
   * connection pools.
   *
   * @see GenericObjectPool#getTestOnReturn()
   * @see GenericKeyedObjectPool#getTestOnReturn()
   */
  public static final String CONNECTION_POOL_TEST_ON_RETURN = "org.plasma.sdo.access.provider.hbase.ConnectionPoolTestOnReturn";

  /**
   * The property name for the {@code testWhileIdle} configuration attribute for
   * connection pools.
   */
  public static final String CONNECTION_POOL_TEST_WHILE_IDLE = "org.plasma.sdo.access.provider.hbase.ConnectionPoolTestWhileIdle";

  /**
   * The property name for the {@code timeBetweenEvictionRunsMillis}
   * configuration attribute for connection pools.
   */
  public static final String CONNECTION_POOL_TIME_BETWEEN_EVICTION_RUNS_MILLIS = "org.plasma.sdo.access.provider.hbase.ConnectionPoolTimeBetweenEvictionRunsMillis";

  /**
   * The property name for the {@code evictionPolicyClassName} configuration
   * attribute for connection pools.
   */
  public static final String CONNECTION_POOL_EVICTION_POLICY_CLASS_NAME = "org.plasma.sdo.access.provider.hbase.ConnectionPoolEvictionPolicyClassName";

  /**
   * The property name for the {@code blockWhenExhausted} configuration
   * attribute for connection pools.
   */
  public static final String CONNECTION_POOL_BLOCK_WHEN_EXHAUSTED = "org.plasma.sdo.access.provider.hbase.ConnectionPoolBlockWhenExhausted";

  /**
   * The property name of the flag that determines if JMX will be enabled for
   * connection pools.
   *
   */
  public static final String CONNECTION_POOL_JMX_ENABLED = "org.plasma.sdo.access.provider.hbase.ConnectionPoolJmxEnabled";

  /**
   * The property name of the JMX name base that will be used as part of the
   * name assigned to JMX enabled pools created with this configuration
   * instance. A value of <code>null</code> means that the pool will define the
   * JMX name base.
   */
  public static final String CONNECTION_POOL_JMX_NAME_BASE = "org.plasma.sdo.access.provider.hbase.ConnectionPoolJmxNameBase";

  /**
   * The property name of the JMX name prefix that will be used as part of the
   * name assigned to JMX enabled pools created with this configuration
   * instance.
   */
  public static final String CONNECTION_POOL_JMX_NAME_PREFIX = "org.plasma.sdo.access.provider.hbase.ConnectionPoolJmxNamePrefix";

  private GenericObjectPool<Connection> pool;

  private static final Log log = LogFactory.getLog(HBaseConnectionManager.class);

  private static volatile HBaseConnectionManager instance;
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

    PooledConnectionFactory factory = new PooledConnectionFactory(this.config);
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

  public static HBaseConnectionManager instance() {
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

  public Connection getConnection() {
    try {
      return this.pool.borrowObject();
    } catch (Exception e) {
      throw new GraphServiceException(e);
    } finally {
    }
  }

  public void createTable(Connection connection, TableName name) {

    Admin admin = null;
    try {
      admin = connection.getAdmin();
      TableMapping tableConfig = StoreMapping.getInstance().getTable(name.getNamespaceAsString(),
          name.getNameAsString(), new StoreMappingContext()); // FIXME:
      HTableDescriptor tableDesc = new HTableDescriptor(name);
      HColumnDescriptor fam1 = new HColumnDescriptor(tableConfig.getDataColumnFamilyName()
          .getBytes());
      tableDesc.addFamily(fam1);
      try {
        admin.createTable(tableDesc);
      } catch (NamespaceNotFoundException nnf) {
        NamespaceDescriptor namespace = NamespaceDescriptor.create(name.getNamespaceAsString())
            .addConfiguration("Description", "cloudgraph generated namespace").build();
        admin.createNamespace(namespace);
        admin.createTable(tableDesc);
      }
    } catch (MasterNotRunningException e1) {
      throw new GraphServiceException(e1);
    } catch (ZooKeeperConnectionException e1) {
      throw new GraphServiceException(e1);
    } catch (IOException e) {
      throw new GraphServiceException(e);
    } finally {
      if (admin != null)
        try {
          admin.close();
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
    }
  }

  public void deleteTable(Connection connection, TableName name) {

    Admin admin = null;
    try {
      admin = connection.getAdmin();
      admin.disableTable(name);
      admin.deleteTable(name);
    } catch (MasterNotRunningException e1) {
      throw new GraphServiceException(e1);
    } catch (ZooKeeperConnectionException e1) {
      throw new GraphServiceException(e1);
    } catch (IOException e) {
      throw new GraphServiceException(e);
    } finally {
      if (admin != null)
        try {
          admin.close();
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
    }
  }

}
