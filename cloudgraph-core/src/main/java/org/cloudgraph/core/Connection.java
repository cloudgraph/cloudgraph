package org.cloudgraph.core;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.cloudgraph.core.client.Admin;
import org.cloudgraph.core.client.BufferedMutator;
import org.cloudgraph.core.client.RegionLocator;
import org.cloudgraph.core.client.Table;
import org.cloudgraph.core.client.TableName;

public interface Connection {

  public abstract void close() throws IOException;

  public abstract void destroy() throws IOException;

  public abstract boolean isClosed();

  public abstract void abort(String why, Throwable e);

  public abstract boolean isAborted();

  public abstract boolean tableExists(TableName tableName) throws IOException;

  public abstract Table getTable(TableName tableName) throws IOException;

  public abstract Table getTable(TableName tableName, ExecutorService pool) throws IOException;

  public abstract RegionLocator getRegionLocator(TableName tableName) throws IOException;

  public abstract BufferedMutator getBufferedMutator(TableName tableName) throws IOException;

  public abstract Admin getAdmin() throws IOException;

}