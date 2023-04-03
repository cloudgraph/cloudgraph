package org.cloudgraph.maprdb.service;

import java.util.Properties;

import org.cloudgraph.core.ConnectionManager;
import org.cloudgraph.core.DataConverter;
import org.cloudgraph.core.ServiceContext;
import org.cloudgraph.core.client.ClientFactory;
import org.cloudgraph.core.filter.ColumnFilterFactory;
import org.cloudgraph.core.filter.RowFilterFactory;
import org.cloudgraph.core.scan.RowKeyFactory;
import org.cloudgraph.hbase.client.HBaseClientFactory;
import org.cloudgraph.hbase.connect.HBaseConnectionManager;
import org.cloudgraph.hbase.filter.HBaseColumnFilterFactory;
import org.cloudgraph.hbase.filter.HBaseRowFilterFactory;
import org.cloudgraph.hbase.scan.HBaseRowKeyFactory;
import org.cloudgraph.hbase.service.HBaseDataConverter;
import org.cloudgraph.maprdb.client.MaprDBClientFactory;
import org.cloudgraph.store.mapping.StoreMappingContext;

public class MaprDBServiceContext implements ServiceContext {

  private Properties properties;
  private StoreMappingContext mappingContext;
  private DataConverter dataConverter;
  private ConnectionManager connectionManager;
  private ClientFactory clientFactory;
  private RowKeyFactory rowKeyFactory;
  private ColumnFilterFactory columnFilterFactory;
  private RowFilterFactory rowFilterFactory;

  public MaprDBServiceContext(Properties properties) {
    super();
    this.properties = properties;
    this.mappingContext = new StoreMappingContext(properties);
    this.dataConverter = HBaseDataConverter.INSTANCE;
    this.connectionManager = HBaseConnectionManager.instance();
    this.clientFactory = new MaprDBClientFactory();
    this.rowKeyFactory = new HBaseRowKeyFactory();
    this.columnFilterFactory = new HBaseColumnFilterFactory();
    this.rowFilterFactory = new HBaseRowFilterFactory();
  }

  public MaprDBServiceContext() {
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
