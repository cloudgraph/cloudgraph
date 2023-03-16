package org.cloudgraph.rocksdb.expr;

import org.cloudgraph.store.service.GraphServiceException;

public class UnsupportedExpressionDataType extends GraphServiceException {

  private static final long serialVersionUID = 1L;

  public UnsupportedExpressionDataType(String message, Throwable cause) {
    super(message, cause);
  }

  public UnsupportedExpressionDataType(String message) {
    super(message);
  }

  public UnsupportedExpressionDataType(Throwable t) {
    super(t);
  }

}
