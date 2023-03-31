package org.cloudgraph.core.client;

import java.io.IOException;

public interface ResultScanner extends Iterable<Result> {

  void close();

  Result next() throws IOException;

}
