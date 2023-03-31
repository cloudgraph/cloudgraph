package org.cloudgraph.hbase.client;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.cloudgraph.core.client.Table;
import org.cloudgraph.core.client.TableName;

public interface SomeAdmin {

  public abstract void close() throws IOException;

  public abstract void destroy() throws IOException;

  public abstract boolean isClosed();

  public abstract void abort(String why, Throwable e);

  public abstract boolean isAborted();

  public abstract Configuration getConfiguration();

  public abstract boolean tableExists(TableName tableName) throws IOException;

  public abstract Table getTable(TableName tableName) throws IOException;

  public abstract Table getTable(TableName tableName, ExecutorService pool) throws IOException;

  public abstract BufferedMutator getBufferedMutator(TableName tableName) throws IOException;

  public abstract BufferedMutator getBufferedMutator(BufferedMutatorParams params)
      throws IOException;

  public abstract RegionLocator getRegionLocator(TableName tableName) throws IOException;

  public abstract Admin getAdmin() throws IOException;

}