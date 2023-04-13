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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.ObjectPool;
import org.cloudgraph.core.Connection;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.Admin;
import org.cloudgraph.core.client.BufferedMutator;
import org.cloudgraph.core.client.RegionLocator;
import org.cloudgraph.core.client.Table;
import org.cloudgraph.core.client.TableName;
import org.cloudgraph.rocksdb.ext.RocksDBTable;
import org.cloudgraph.rocksdb.service.CloudGraphContext;
import org.cloudgraph.store.mapping.StoreMapping;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.StoreMappingProp;
import org.cloudgraph.store.mapping.TableMapping;
import org.cloudgraph.store.service.GraphServiceException;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Filter;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.HashSkipListMemTableConfig;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.VectorMemTableConfig;
import org.rocksdb.util.SizeUnit;

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
public class RocksDBConnection implements Connection {
  private static Log log = LogFactory.getLog(RocksDBConnection.class);
  private ObjectPool<Connection> pool;
  private LoadingCache<TableName, RocksDBTable> tableCache;
  private RocksDB connection;
  private ServiceContext serviceContext;
  private List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

  public RocksDBConnection(ObjectPool<Connection> pool, ServiceContext serviceContext) {
    super();
    this.pool = pool;
    this.serviceContext = serviceContext;
    final Options options = new Options();
    final Filter bloomFilter = new BloomFilter(10);
    final ReadOptions readOptions = new ReadOptions().setFillCache(false);

    final Statistics stats = new Statistics();
    final RateLimiter rateLimiter = new RateLimiter(10000000, 10000, 10);
    options.setCreateIfMissing(true);
    options.setStatistics(stats);
    options.setWriteBufferSize(8 * SizeUnit.KB);
    options.setMaxWriteBufferNumber(3);
    options.setMaxBackgroundJobs(10);
    options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
    options.setCompactionStyle(CompactionStyle.UNIVERSAL);
    options.setMemTableConfig(new HashSkipListMemTableConfig().setHeight(4).setBranchingFactor(4)
        .setBucketCount(2000000));
    options.setMergeOperatorName("uint64add");

    // options.setMemTableConfig(new
    // HashLinkedListMemTableConfig().setBucketCount(100000));
    // options.setMemTableConfig(new
    // VectorMemTableConfig().setReservedSize(10000));
    // options.setMemTableConfig(new SkipListMemTableConfig());

    PlainTableConfig tableConfig = new PlainTableConfig();
    options.setTableFormatConfig(tableConfig);

    // Plain-Table requires mmap read
    options.setAllowMmapReads(true);

    options.setRateLimiter(rateLimiter);

    final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
    Cache cache = new LRUCache(64 * 1024, 6);
    tableOptions.setBlockCache(cache);
    tableOptions.setFilterPolicy(bloomFilter);
    tableOptions.setBlockSizeDeviation(5);
    tableOptions.setBlockRestartInterval(10);
    tableOptions.setCacheIndexAndFilterBlocks(true);
    tableOptions.setBlockCacheCompressed(new LRUCache(64 * 1000, 10));

    byte[] cfBytes = "f1".getBytes();
    ColumnFamilyDescriptor cf = new ColumnFamilyDescriptor(cfBytes, new ColumnFamilyOptions());

    RocksDB db;
    try {
      Options createOptions = new Options();
      createOptions.setCreateIfMissing(true);
      // createOptions.setMergeOperatorName("uint64add");
      List<byte[]> list = RocksDB.listColumnFamilies(createOptions, "/home/rocksdbdata");
      boolean found = false;
      for (byte[] famBytes : list) {
        if (Arrays.equals(famBytes, cfBytes)) {
          found = true;
          break;
        }
      }
      if (!found) {
        db = RocksDB.open(createOptions, "/home/rocksdbdata");
        db.createColumnFamily(cf);
        db.close();
      }
    } catch (RocksDBException e1) {
      throw new GraphServiceException(e1);
    }

    DBOptions dbOptions = new DBOptions();
    dbOptions.setCreateIfMissing(true);
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    // have to open default column family
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
        new ColumnFamilyOptions()));

    // open the new one, too
    columnFamilyDescriptors.add(cf);

    try {
      this.connection = RocksDB.open(dbOptions, "/home/rocksdbdata", columnFamilyDescriptors,
          columnFamilyHandles);
    } catch (RocksDBException e) {
      throw new GraphServiceException(e);
    }
    final int cacheMax = StoreMappingProp.getConnectionTablecacheSizeMax();
    final int cacheTimeout = StoreMappingProp.getConnectionTablecacheTimeoutSeconds();
    Map<String, String> propsMap = StoreMappingProp.getConnectionTableConfigProperties();
    Iterator<String> keys = propsMap.keySet().iterator();
    while (keys.hasNext()) {
      String key = keys.next();
      String value = propsMap.get(key);
    }

    this.tableCache = CacheBuilder.newBuilder().maximumSize(cacheMax)
        .expireAfterAccess(cacheTimeout, TimeUnit.SECONDS)
        .removalListener(new RemovalListener<TableName, RocksDBTable>() {
          @Override
          public void onRemoval(RemovalNotification<TableName, RocksDBTable> event) {
          }
        }).build(new CacheLoader<TableName, RocksDBTable>() {
          @Override
          public RocksDBTable load(TableName tableName) throws Exception {
            if (log.isDebugEnabled())
              log.debug("loading table " + this + " " + tableName.getNamespace() + "."
                  + tableName.getTableName());
            StringBuilder logicalTableNameKey = new StringBuilder();
            if (tableName.getNamespace() != null) {
              logicalTableNameKey.append(tableName.getNamespace());
              logicalTableNameKey.append("/");
            }
            logicalTableNameKey.append(tableName.getTableName());
            // // Uses a path as the single key for internal table mapping
            // String qualifiedLogicalName = StoreMapping.getInstance()
            // .qualifiedLogicalTableNameFromPhysicalTablePath(null,
            // logicalTableNameKey.toString(), serviceContext);

            TableMapping tableConfig = StoreMapping.getInstance().getTableByQualifiedLogicalName(
                logicalTableNameKey.toString(), serviceContext.getStoreMapping());

            return new RocksDBTable(tableName, connection, tableConfig, serviceContext);
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
    for (ColumnFamilyHandle handle : columnFamilyHandles) {
      handle.close();
    }
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

  @Override
  public Table getTable(TableName tableName) throws IOException {
    Table result = null;
    try {
      result = this.tableCache.get(tableName);
    } catch (ExecutionException e) {
      log.error(e.getMessage(), e);
    }
    return result;
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
    Table result = null;
    try {
      result = this.tableCache.get(tableName);
    } catch (ExecutionException e) {
      log.error(e.getMessage(), e);
    }
    return result;
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Admin getAdmin() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
