package org.cloudgraph.hbase.service;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.plasma.sdo.PlasmaDataGraph;

/**
 * Stores intermediate results under various single and multi-scan contexts in
 * order to fulfill a query and may conditionally bypass results avoiding the
 * overhead of assembling a graph.
 * 
 * @author Scott Cinnamond
 * @since 0.5.9
 */
public interface ResultsAssembler {
  /**
   * Returns results
   * 
   * @return
   */
  public PlasmaDataGraph[] getResults();

  /**
   * Returns the current size of the collection
   * 
   * @return the current size of the collection
   */
  public int size();

  /**
   * Returns whether results may be ignored under the current context.
   * 
   * @return whether results may be ignored under the current context.
   */
  public boolean canIgnoreResults();

  public int getUnrecognizedResults();

  public int getIgnoredResults();

  public boolean isResultEndRangeReached();

  /**
   * Determines whether the given row is to be added to the underlying
   * collection returning true if the given row is added and false if the given
   * row is not added.
   * 
   * @param resultRow
   *          the row
   * @return true if the given row is added and false if the given row is not
   *         added.
   * @throws IOException
   */
  public boolean collect(Result resultRow) throws IOException;
}
