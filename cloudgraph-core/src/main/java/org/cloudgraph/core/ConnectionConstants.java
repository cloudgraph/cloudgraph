package org.cloudgraph.core;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

public interface ConnectionConstants {
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

}
