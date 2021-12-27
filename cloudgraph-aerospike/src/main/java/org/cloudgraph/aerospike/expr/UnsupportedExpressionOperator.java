package org.cloudgraph.aerospike.expr;

import org.cloudgraph.store.service.GraphServiceException;

public class UnsupportedExpressionOperator extends GraphServiceException {

  private static final long serialVersionUID = 1L;

  public UnsupportedExpressionOperator(String message, Throwable cause) {
    super(message, cause);
  }

  public UnsupportedExpressionOperator(String message) {
    super(message);
  }

  public UnsupportedExpressionOperator(Throwable t) {
    super(t);
  }

}
