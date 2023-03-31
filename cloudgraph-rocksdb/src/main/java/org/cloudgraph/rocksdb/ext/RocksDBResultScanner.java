package org.cloudgraph.rocksdb.ext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.cloudgraph.core.client.Result;
import org.cloudgraph.core.client.ResultScanner;

public class RocksDBResultScanner implements ResultScanner {
  List<RocksDBResult> results = new ArrayList<>();

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
    // TODO Auto-generated method stub

  }

  @Override
  public Result next() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
