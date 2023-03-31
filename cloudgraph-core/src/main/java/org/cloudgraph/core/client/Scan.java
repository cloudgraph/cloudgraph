package org.cloudgraph.core.client;

import java.io.IOException;

public interface Scan {

  Filter getFilter();

  void setStartRow(byte[] firstRow);

  void setStopRow(byte[] row);

  byte[] getStartRow();

  byte[] getStopRow();

  void setTimeStamp(long parseLong) throws IOException;

  void setTimeRange(long parseLong, long parseLong2) throws IOException;

  void setMaxVersions(int parseInt);

  void setCaching(int parseInt);

  void setCacheBlocks(boolean b);

}
