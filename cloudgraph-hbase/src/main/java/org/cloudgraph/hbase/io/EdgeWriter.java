package org.cloudgraph.hbase.io;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaEdge;
import org.plasma.sdo.PlasmaNode;

public interface EdgeWriter extends EdgeOperation {

  public abstract RowWriter getRowWriter();

  public abstract void replace(PlasmaDataObject oldValue, PlasmaDataObject newValue)
      throws IOException;

  public abstract void remove(PlasmaDataObject oldValue) throws IOException;

  public abstract void add(PlasmaDataObject newValue) throws IOException;

  // create operation specific?
  public abstract void addAll(PlasmaNode dataNode, List<PlasmaEdge> edges) throws IOException;

  /**
   * Determines which object the user intentionally removed and/or added based
   * on the change summary of the client graph. Then removes and/or adds to the
   * existing collection, just what the user has indicated.
   * <p>
   * </p>
   * Note: the client edge collection can be the results from a slice query
   * where not all the collection results are returned. We don't want to remove
   * what we DON'T find in the commit graph in this case.
   * 
   * @param oldValueList
   * @param currentEdges
   * @throws IOException
   */
  public abstract void merge(PlasmaNode dataNode, HashSet<PlasmaDataObject> oldValues,
      List<PlasmaEdge> currentEdges) throws IOException;

  public abstract void write() throws IOException;

  // Flag it deleted and write the data on exit
  public abstract void delete() throws IOException;

}