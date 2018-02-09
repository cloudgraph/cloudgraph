/**
 * Copyright 2017 TerraMeta Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudgraph.hbase.mapreduce;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;
import org.cloudgraph.hbase.util.FilterUtil;
import org.cloudgraph.mapreduce.GraphWritable;
import org.cloudgraph.store.mapping.CloudGraphStoreMapping;

/**
 * A graph based input-specification for MapReduce jobs which splits an
 * underlying root table by region and by the scans resulting from a given <a
 * href="http://plasma-sdo.org/org/plasma/query/Query.html">query</a>, then
 * provides graph record {@link GraphRecordReader readers} for each <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableSplit.html"
 * >split</a> which assemble and serve data graphs to client {@link GraphMapper
 * mapper} extensions.
 * <p>
 * Data graphs are assembled within a record {@link GraphRecordReader reader}
 * based on the detailed selection criteria within a given <a
 * href="http://plasma-sdo.org/org/plasma/query/Query.html">query</a>, and may
 * be passed to a {@link GraphRecordRecognizer recognizer} and potentially
 * screened from client {@link GraphMapper mappers} potentially illuminating
 * business logic dedicated to identifying specific records.
 * </p>
 * <p>
 * A graph recognizer is used only when query expressions are present which
 * reference properties not found in the row key model for a target <a
 * href="http://plasma-sdo.org/commonj/sdo/DataGraph.html">graph</a>.
 * </p>
 * 
 * @see org.apache.hadoop.hbase.mapreduce.TableSplit
 * @see GraphRecordReader
 * @see GraphRecordRecognizer
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public class GraphInputFormat extends InputFormat<ImmutableBytesWritable, GraphWritable> implements
    Configurable {

  static final Log log = LogFactory.getLog(GraphInputFormat.class);

  /**
   * Serialized query which encapsulates meta data which drives the creation is
   * one or more scans against any number of tables, starting with the root.
   */
  public static final String QUERY = "cloudgraph.hbase.mapreduce.query";

  /**
   * Serialized store mappings or configuration which links physical tables with
   * any number of data graphs/mappings. This data is typically packaged into a
   * jar in an e.g. cloudgraph-config.xml file, but where these mappings are
   * dynamic and loaded at runtime, the job (spark,mapreduce) child VM's must
   * have access to and load this mapping dynamically. The mapping must be
   * serialized as XML and be marshalled and be unmarshallable using the root
   * {@link CloudGraphStoreMapping} which may have any number of table mapping
   * elements.
   */
  public static final String TABLE_MAPPINGS = "cloudgraph.hbase.mapreduce.tablemappings";

  /**
   * Boolean property indicating whether a graph recognizer is necessary for the
   * current query
   */
  public static final String RECOGNIZER = "cloudgraph.hbase.mapreduce.recognizer";

  /** Internal Job parameter that specifies the scan list. */
  protected static final String SCANS = "cloudgraph.hbase.mapreduce.scans";

  /**
   * Internal Job parameter that specifies root the input table as derived from
   * a deserialized query
   */
  protected static final String ROOT_TABLE = "cloudgraph.hbase.mapreduce.roottable";

  /** The timestamp used to filter columns with a specific timestamp. */
  protected static final String SCAN_TIMESTAMP = "cloudgraph.hbase.mapreduce.scan.timestamp";
  /**
   * The starting timestamp used to filter columns with a specific range of
   * versions.
   */
  protected static final String SCAN_TIMERANGE_START = "cloudgraph.hbase.mapreduce.scan.timerange.start";
  /**
   * The ending timestamp used to filter columns with a specific range of
   * versions.
   */
  protected static final String SCAN_TIMERANGE_END = "cloudgraph.hbase.mapreduce.scan.timerange.end";
  /** The maximum number of version to return. */
  protected static final String SCAN_MAXVERSIONS = "cloudgraph.hbase.mapreduce.scan.maxversions";

  /** Set to false to disable server-side caching of blocks for this scan. */
  public static final String SCAN_CACHEBLOCKS = "cloudgraph.hbase.mapreduce.scan.cacheblocks";
  /** The number of rows for caching that will be passed to scanners. */
  public static final String SCAN_CACHEDROWS = "cloudgraph.hbase.mapreduce.scan.cachedrows";

  /** The configuration. */
  private Configuration conf = null;

  /** The {@link Admin}. */
  private Admin admin;
  /** The root {@link Table} to scan. */
  private Table table;
  /** The {@link RegionLocator} of the table. */
  private RegionLocator regionLocator;
  /** The reader scanning the table, can be a custom one. */
  private TableRecordReader tableRecordReader = null;
  /** The underlying {@link Connection} of the table. */
  private Connection connection;

  /** Holds the set of scans used to define the input. */
  private List<Scan> scans;

  /** The reader scanning the table, can be a custom one. */
  private GraphRecordReader graphRecordReader = null;

  private HashMap<InetAddress, String> reverseDNSCache = new HashMap<InetAddress, String>();

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    // String tableName = conf.get(ROOT_TABLE);
    TableName tableName = TableName.valueOf(conf.get(ROOT_TABLE));
    try {
      // table = new HTable(new Configuration(conf), tableName);
      Connection con = ConnectionFactory.createConnection(new Configuration(conf));
      this.table = con.getTable(tableName);
      this.regionLocator = con.getRegionLocator(tableName);
      this.admin = con.getAdmin();
      this.connection = con;
    } catch (Exception e) {
      log.error(StringUtils.stringifyException(e));
    }

    String[] rawScans = conf.getStrings(SCANS);
    if (rawScans.length <= 0) {
      throw new IllegalArgumentException("There must be at least 1 scan configuration set to : "
          + SCANS);
    }
    List<Scan> scans = new ArrayList<Scan>();

    for (int i = 0; i < rawScans.length; i++) {
      try {
        Scan scan = GraphMapReduceSetup.convertStringToScan(rawScans[i]);
        setConf(scan, configuration);
        scans.add(scan);
      } catch (IOException e) {
        throw new RuntimeException("Failed to convert Scan : " + rawScans[i] + " to string", e);
      }
    }
    this.setScans(scans);
  }

  private void setConf(Scan scan, Configuration configuration) throws NumberFormatException,
      IOException {
    if (conf.get(SCAN_TIMESTAMP) != null) {
      scan.setTimeStamp(Long.parseLong(conf.get(SCAN_TIMESTAMP)));
    }

    if (conf.get(SCAN_TIMERANGE_START) != null && conf.get(SCAN_TIMERANGE_END) != null) {
      scan.setTimeRange(Long.parseLong(conf.get(SCAN_TIMERANGE_START)),
          Long.parseLong(conf.get(SCAN_TIMERANGE_END)));
    }

    if (conf.get(SCAN_MAXVERSIONS) != null) {
      scan.setMaxVersions(Integer.parseInt(conf.get(SCAN_MAXVERSIONS)));
    }

    if (conf.get(SCAN_CACHEDROWS) != null) {
      scan.setCaching(Integer.parseInt(conf.get(SCAN_CACHEDROWS)));
    }

    // false by default, full table scans generate too much GC churn
    scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    if (scans.isEmpty()) {
      throw new IOException("No scans were provided.");
    }

    try {
      RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(regionLocator, admin);
      Pair<byte[][], byte[][]> keys = getStartEndKeys();
      // if potentially a single split (or table is empty?)
      if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
        HRegionLocation regLoc = regionLocator
            .getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, false);
        if (null == regLoc) {
          throw new IOException("Expecting at least one region.");
        }
        List<InputSplit> splits = new ArrayList<InputSplit>(1);
        long regionSize = sizeCalculator.getRegionSize(regLoc.getRegionInfo().getRegionName());
        Scan scan = scans.get(0);
        if (scans.size() > 1)
          log.warn("single split with multiple scans - ignoring other than first scan");

        TableSplit split = new TableSplit(table.getName(), scan, HConstants.EMPTY_BYTE_ARRAY,
            HConstants.EMPTY_BYTE_ARRAY, regLoc.getHostnamePort().split(
                Addressing.HOSTNAME_PORT_SEPARATOR)[0], regionSize);
        splits.add(split);
        return splits;
      }

      List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
      for (Scan scan : scans) {

        for (int i = 0; i < keys.getFirst().length; i++) {
          if (!includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
            continue;
          }
          HRegionLocation location = regionLocator.getRegionLocation(keys.getFirst()[i], false);
          // The below InetSocketAddress creation does a name
          // resolution.
          InetSocketAddress isa = new InetSocketAddress(location.getHostname(), location.getPort());
          if (isa.isUnresolved()) {
            log.error("Failed to resolve host: " + isa + " - ignoring entire split for this host!");
            continue;
          }
          InetAddress regionAddress = isa.getAddress();
          String regionLocation;
          try {
            regionLocation = reverseDNS(regionAddress);
          } catch (NamingException e) {
            log.warn("Cannot resolve the host name for " + regionAddress + " because of " + e);
            regionLocation = location.getHostname();
          }

          byte[] startRow = scan.getStartRow();
          byte[] stopRow = scan.getStopRow();
          // determine if the given start an stop key fall into the
          // region.
          if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes.compareTo(startRow,
              keys.getSecond()[i]) < 0)
              && (stopRow.length == 0 || Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
            byte[] splitStart = startRow.length == 0
                || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys.getFirst()[i]
                : startRow;
            byte[] splitStop = (stopRow.length == 0 || Bytes
                .compareTo(keys.getSecond()[i], stopRow) <= 0) && keys.getSecond()[i].length > 0 ? keys
                .getSecond()[i] : stopRow;

            byte[] regionName = location.getRegionInfo().getRegionName();
            long regionSize = sizeCalculator.getRegionSize(regionName);

            TableSplit split = new TableSplit(table.getName(), scan, // must
                                                                     // include
                                                                     // the scan
                                                                     // as it
                                                                     // may have
                // various filters
                splitStart, splitStop, regionLocation, regionSize);
            splits.add(split);
            if (log.isDebugEnabled()) {
              log.debug("getSplits: split -> " + i + " -> " + split);
            }
          }
        }
      }
      return splits;
    } finally {
      closeAll();
    }
  }

  /**
   * Uses {@link InetAddress} in case of {@link DNS} lookup failure.
   * 
   * @param ipAddr
   *          the address
   * @return the reverse address
   * @throws NamingException
   * @throws UnknownHostException
   */
  public String reverseDNS(InetAddress ipAddr) throws NamingException, UnknownHostException {
    String hostName = this.reverseDNSCache.get(ipAddr);
    if (hostName == null) {
      String ipAddressString = null;
      try {
        ipAddressString = DNS.reverseDns(ipAddr, null);
      } catch (Exception e) {
        ipAddressString = InetAddress.getByName(ipAddr.getHostAddress()).getHostName();
      }
      if (ipAddressString == null)
        throw new UnknownHostException("No host found for " + ipAddr);
      hostName = Strings.domainNamePointerToHostName(ipAddressString);
      this.reverseDNSCache.put(ipAddr, hostName);
    }
    return hostName;
  }

  private void closeAll() throws IOException {
    close(admin, table, regionLocator, connection);
    admin = null;
    table = null;
    regionLocator = null;
    connection = null;
  }

  private void close(Closeable... closables) throws IOException {
    for (Closeable c : closables) {
      if (c != null) {
        c.close();
      }
    }
  }

  protected Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    return this.regionLocator.getStartEndKeys();
  }

  @Override
  public RecordReader<ImmutableBytesWritable, GraphWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    if (table == null) {
      throw new IOException("Cannot create a record reader because of a"
          + " previous error. Please look at the previous logs lines from"
          + " the task's full log for more details.");
    }
    TableSplit tSplit = (TableSplit) split;
    GraphRecordReader reader = this.graphRecordReader;
    // if no record reader was provided use default
    if (reader == null) {
      reader = new GraphRecordReader();
    }
    Scan sc = tSplit.getScan();
    log.debug("SCAN: " + sc.toString());
    if (sc.getFilter() != null) {
      log.info("SPLIT FILTER: " + FilterUtil.printFilterTree(sc.getFilter()));
    } else {
      log.info("split scan has no filter");
    }

    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    reader.setScan(sc);
    reader.setTable(table);
    try {
      reader.initialize(tSplit, context);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
    return reader;
  }

  /**
   * Test if the given region is to be included in the InputSplit while
   * splitting the regions of a table.
   * <p>
   * This optimization is effective when there is a specific reasoning to
   * exclude an entire region from the M-R job, (and hence, not contributing to
   * the InputSplit), given the start and end keys of the same. <br>
   * Useful when we need to remember the last-processed top record and revisit
   * the [last, current) interval for M-R processing, continuously. In addition
   * to reducing InputSplits, reduces the load on the region server as well, due
   * to the ordering of the keys. <br>
   * <br>
   * Note: It is possible that <code>endKey.length() == 0 </code> , for the last
   * (recent) region. <br>
   * Override this method, if you want to bulk exclude regions altogether from
   * M-R. By default, no region is excluded( i.e. all regions are included).
   * 
   * @param startKey
   *          Start key of the region
   * @param endKey
   *          End key of the region
   * @return true, if this region needs to be included as part of the input
   *         (default).
   */
  protected boolean includeRegionInSplit(final byte[] startKey, final byte[] endKey) {
    return true;
  }

  /**
   * Allows subclasses to get the list of {@link Scan} objects.
   */
  protected List<Scan> getScans() {
    return this.scans;
  }

  /**
   * Allows subclasses to set the list of {@link Scan} objects.
   * 
   * @param scans
   *          The list of {@link Scan} used to define the input
   */
  protected void setScans(List<Scan> scans) {
    this.scans = scans;
  }
}
