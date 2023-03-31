package org.cloudgraph.core;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.cloudgraph.core.client.TableName;
import org.cloudgraph.store.mapping.StoreMappingContext;

public interface ConnectionManager {

  public abstract Connection getConnection();

  public abstract void createTable(Connection connection, TableName name,
      StoreMappingContext mappingContext);

  public abstract void deleteTable(Connection connection, TableName name);

}