package org.cloudgraph.hbase.client;

import java.io.IOException;
import java.util.Iterator;

import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.client.ResultScanner;

public class HBaseResultScanner implements ResultScanner {
  private org.apache.hadoop.hbase.client.ResultScanner scanner;

  @SuppressWarnings("unused")
  private HBaseResultScanner() {
  }

  public HBaseResultScanner(org.apache.hadoop.hbase.client.ResultScanner scanner) {
    super();
    this.scanner = scanner;
  }

  public org.apache.hadoop.hbase.client.ResultScanner get() {
    return scanner;
  }

  @Override
  public Iterator<Result> iterator() {
    return new Iterator<Result>() {
      final Iterator<org.apache.hadoop.hbase.client.Result> iter = scanner.iterator();

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Result next() {
        return new HBaseResult(iter.next());
      }
    };
  }

  @Override
  public void close() {
    this.scanner.close();

  }

  @Override
  public Result next() throws IOException {
    return new HBaseResult(this.scanner.next());
  }
}
