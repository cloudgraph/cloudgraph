package org.cloudgraph.core.client;

import java.io.IOException;

public interface RowMutations {

  void add(Put cast) throws IOException;

  void add(Delete cast) throws IOException;

}
