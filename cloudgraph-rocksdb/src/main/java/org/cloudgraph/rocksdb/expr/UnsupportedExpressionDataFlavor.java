package org.cloudgraph.rocksdb.expr;

import org.cloudgraph.store.service.GraphServiceException;

public class UnsupportedExpressionDataFlavor extends GraphServiceException {

  private static final long serialVersionUID = 1L;

  public UnsupportedExpressionDataFlavor(String message, Throwable cause) {
    super(message, cause);
  }

  public UnsupportedExpressionDataFlavor(String message) {
    super(message);
  }

  public UnsupportedExpressionDataFlavor(Throwable t) {
    super(t);
  }

}
