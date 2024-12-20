package org.cloudgraph.rocksdb.service;

import java.util.Properties;

import org.cloudgraph.core.ConnectionManager;
import org.cloudgraph.core.DataConverter;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.ClientFactory;
import org.cloudgraph.core.filter.ColumnFilterFactory;
import org.cloudgraph.core.filter.RowFilterFactory;
import org.cloudgraph.core.scan.RowKeyFactory;
import org.cloudgraph.rocksdb.client.RocksDBClientFactory;
import org.cloudgraph.rocksdb.connect.RocksDBConnectionManager;
import org.cloudgraph.rocksdb.filter.RocksDBColumnFilterFactory;
import org.cloudgraph.rocksdb.filter.RocksDBRowFilterFactory;
import org.cloudgraph.rocksdb.scan.RocksDBRowKeyFactory;
import org.cloudgraph.store.mapping.StoreMappingContext;
import org.cloudgraph.store.mapping.TableMapping;

public class RocksDBServiceContext implements ServiceContext {

  private Properties properties;
  private StoreMappingContext mappingContext;
  private DataConverter dataConverter;
  private ConnectionManager connectionManager;
  private ClientFactory clientFactory;
  private RowKeyFactory rowKeyFactory;
  private ColumnFilterFactory columnFilterFactory;
  private RowFilterFactory rowFilterFactory;

  public RocksDBServiceContext(Properties properties) {
    super();
    this.properties = properties;
    this.mappingContext = new StoreMappingContext(properties);
    this.dataConverter = RocksDBDataConverter.INSTANCE;
    this.connectionManager = RocksDBConnectionManager.instance();
    this.clientFactory = new RocksDBClientFactory();
    this.rowKeyFactory = new RocksDBRowKeyFactory();
    this.columnFilterFactory = new RocksDBColumnFilterFactory();
    this.rowFilterFactory = new RocksDBRowFilterFactory();
  }

  @Override
  public String getNamespaceQualifiedPhysicalName(TableMapping tableConfig,
      StoreMappingContext storeMapping) {
    return this.clientFactory.getNamespaceQualifiedPhysicalName(tableConfig, storeMapping);
  }

  public RocksDBServiceContext() {
    this(new Properties());
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public Properties getProperties() {
    return this.properties;
  }

  @Override
  public StoreMappingContext getStoreMapping() {
    return this.mappingContext;
  }

  @Override
  public DataConverter getDataConverter() {
    return this.dataConverter;
  }

  @Override
  public ConnectionManager getConnectionManager() {
    return this.connectionManager;
  }

  @Override
  public ClientFactory getClientFactory() {
    return this.clientFactory;
  }

  @Override
  public RowKeyFactory getRowKeyFactory() {
    return this.rowKeyFactory;
  }

  @Override
  public ColumnFilterFactory getColumnFilterFactory() {
    return this.columnFilterFactory;
  }

  @Override
  public RowFilterFactory getRowFilterFactory() {
    return this.rowFilterFactory;
  }

}
