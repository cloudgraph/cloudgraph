package org.cloudgraph.rocksdb.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.client.ResultScanner;

public class RocksDBResultScanner implements ResultScanner {
  List<RocksDBResult> results = Collections.EMPTY_LIST;

  public RocksDBResultScanner(RocksDBResult[] result) {
    results = new ArrayList<RocksDBResult>(result.length);
    for (RocksDBResult r : result)
      results.add(r);
  }

  @Override
  public Iterator<Result> iterator() {
    return new Iterator<Result>() {
      final Iterator<RocksDBResult> iter = results.iterator();

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Result next() {
        return iter.next();
      }
    };
  }

  @Override
  public void close() {
    this.results = null;

  }

  @Override
  public Result next() throws IOException {
    throw new IllegalStateException("not implemented");
  }

}
