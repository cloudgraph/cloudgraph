package org.cloudgraph.hbase.results;

public interface ResultsSlider {
  /**
   * Returns whether results may be ignored under the current context.
   * 
   * @return whether results may be ignored under the current context.
   */
  public boolean canIgnoreResults();

  public int getUnrecognizedResults();

  public int getIgnoredResults();

  public boolean isResultEndRangeReached();

  boolean currentResultIgnored();

}
