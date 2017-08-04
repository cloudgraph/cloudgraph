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
package org.cloudgraph.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Method;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StringUtils;
import org.cloudgraph.common.CloudGraphConstants;
import org.cloudgraph.store.service.MetricCollector;
import org.plasma.sdo.core.CoreDataObject;
import org.plasma.sdo.xml.DefaultOptions;
import org.plasma.sdo.xml.StreamUnmarshaller;
import org.plasma.sdo.xml.UnmarshallerException;

import commonj.sdo.DataGraph;
import commonj.sdo.helper.XMLDocument;

/**
 * An HDFS XML text file record reader that iterates over HDFS data for the
 * current <code>TableSplit</code>, unmarshalling the XML as structured data
 * graphs based on structural and XML-specific metadata from the underlying
 * domain model. Data graphs may be heterogeneous and of any size or complexity
 * are supplied through {@link GraphXmlInputFormat} including graphs where the
 * underlying domain model contains instances of multiple inheritance. The
 * unmarshalling is stream oriented and leverages the XML (StAX) parser based
 * Plasma <a
 * href="http://plasma-sdo.org/org/plasma/sdo/xml/StreamUnmarshaller.html"
 * >StreamUnmarshaller</a>.
 * <p>
 * Several job {@link Counters} are set up which accumulate various metrics
 * related to the resulting graph, the time taken to unmarshal the XML in
 * addition to other metrics.
 * </p>
 * 
 * This XML text file record reader is "line oriented" such that every HDFS line
 * is assumed to be a single data graph marshalled as XML. Below is an example
 * where a given data graph is being serialized as a single line.
 * 
 * <pre>
 * protected byte[] marshal(DataGraph graph) throws IOException {
 *   DefaultOptions options = new DefaultOptions(graph.getRootObject().getType().getURI());
 *   options.setRootNamespacePrefix(&quot;c&quot;);
 *   options.setPrettyPrint(false);
 *   XMLDocument doc = PlasmaXMLHelper.INSTANCE.createDocument(graph.getRootObject(), graph
 *       .getRootObject().getType().getURI(), null);
 *   doc.setXMLDeclaration(false);
 *   ByteArrayOutputStream os = new ByteArrayOutputStream();
 *   PlasmaXMLHelper.INSTANCE.save(doc, os, options);
 *   os.close();
 *   return os.toByteArray();
 * }
 * </pre>
 * 
 * @see org.cloudgraph.mapreduce.GraphWritable
 * @see org.cloudgraph.mapreduce.GraphXmlInputFormat
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public class GraphXmlRecordReader extends RecordReader<LongWritable, GraphWritable> {

  static final Log log = LogFactory.getLog(GraphXmlRecordReader.class);

  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private int maxLineLength;
  private LongWritable key = new LongWritable();
  private GraphWritable value = null;
  private Configuration configuration;
  private String rootNamespaceUri;
  private String rootNamespacePrefix;
  private DefaultOptions unmarshalOptions;
  private StreamUnmarshaller unmarshaler;
  private TaskAttemptContext context;
  private Method getCounter = null;
  private long totalGraphNodesAssembled = 0;
  private long totalGraphUnmarshalTime = 0;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
      InterruptedException {
    // This InputSplit is a FileInputSplit
    FileSplit split = (FileSplit) inputSplit;

    this.context = context;
    this.configuration = context.getConfiguration();
    this.getCounter = Counters.retrieveGetCounterWithStringsParams(context);

    this.rootNamespaceUri = configuration.get(GraphXmlInputFormat.ROOT_ELEM_NAMESPACE_URI);
    this.rootNamespacePrefix = configuration.get(GraphXmlInputFormat.ROOT_ELEM_NAMESPACE_PREFIX,
        "ns1");

    this.unmarshalOptions = new DefaultOptions(this.rootNamespaceUri);
    this.unmarshalOptions.setRootNamespacePrefix(this.rootNamespacePrefix);
    this.unmarshalOptions.setValidate(false);
    this.unmarshalOptions.setFailOnValidationError(false);
    this.unmarshaler = new StreamUnmarshaller(this.unmarshalOptions, null);

    // Retrieve configuration, and Max allowed
    // bytes for a single record
    this.maxLineLength = configuration.getInt("mapred.linerecordreader.maxlength",
        Integer.MAX_VALUE);

    // Split "S" is responsible for all records
    // starting from "start" and "end" positions
    start = split.getStart();
    end = start + split.getLength();

    // Retrieve file containing Split "S"
    final Path file = split.getPath();
    FileSystem fs = file.getFileSystem(this.configuration);
    FSDataInputStream fileIn = fs.open(split.getPath());

    // If Split "S" starts at byte 0, first line will be processed
    // If Split "S" does not start at byte 0, first line has been already
    // processed by "S-1" and therefore needs to be silently ignored
    boolean skipFirstLine = false;
    if (start != 0) {
      skipFirstLine = true;
      // Set the file pointer at "start - 1" position.
      // This is to make sure we won't miss any line
      // It could happen if "start" is located on a EOL
      --start;
      fileIn.seek(start);
    }

    in = new LineReader(fileIn, this.configuration);

    // If first line needs to be skipped, read first line
    // and stores its content to a dummy Text
    if (skipFirstLine) {
      Text dummy = new Text();
      // Reset "start" to "start + line offset"
      start += in.readLine(dummy, 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
    }

    // Position is the actual start
    this.pos = start;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Current offset is the key
    key.set(pos);

    int newSize = 0;

    // Make sure we get at least one record that starts in this Split
    while (pos < end) {

      Text text = new Text();
      // Read first line and store its content to "value"
      newSize = in.readLine(text, maxLineLength,
          Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));

      DataGraph graph = this.unmarshal(text);
      this.value = new GraphWritable(graph);
      updateCounters();

      // No byte read, seems that we reached end of Split
      // Break and return false (no key / value)
      if (newSize == 0) {
        break;
      }

      // Line is read, new position is set
      pos += newSize;

      // Line is lower than Maximum record line size
      // break and return true (found key / value)
      if (newSize < maxLineLength) {
        break;
      }

      // Line is too long
      // Try again with position = position + line offset,
      // i.e. ignore line and go to next one
      log.error("Skipped line of size " + newSize + " at pos " + (pos - newSize));
    }

    if (newSize == 0) {
      // We've reached end of Split
      key = null;
      value = null;
      return false;
    } else {
      // Tell Hadoop a new line has been found
      // key / value will be retrieved by
      // getCurrentKey getCurrentValue methods
      return true;
    }

  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public GraphWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  /**
   * Updates various job counters.
   * 
   * @throws IOException
   */
  private void updateCounters() throws IOException {
    // we can get access to counters only if hbase uses new mapreduce APIs
    if (this.getCounter == null) {
      return;
    }

    try {
      ((Counter) this.getCounter.invoke(context, Counters.CLOUDGRAPH_COUNTER_GROUP_NAME,
          Counters.CLOUDGRAPH_COUNTER_NAME_NUM_GRAPH_NODES_ASSEMBLED))
          .increment(this.totalGraphNodesAssembled);

      ((Counter) this.getCounter.invoke(context, Counters.CLOUDGRAPH_COUNTER_GROUP_NAME,
          Counters.CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_XML_UNMARSHAL_TIME))
          .increment(this.totalGraphUnmarshalTime);

    } catch (Exception e) {
      log.debug("can't update counter." + StringUtils.stringifyException(e));
    }
  }

  /**
   * Deserializes the given text/xml and unmarshalling it as a data graph,
   * capturing various metrics and returning the new graph. The given text/xml
   * represents a single line in the underlying HDFS file and is assumed to be
   * an XML serialized data graph.
   * 
   * @param text
   *          the input text
   * @return the new graph
   * @throws IOException
   */
  private DataGraph unmarshal(Text text) throws IOException {

    long before = System.currentTimeMillis();

    String textString = text.toString();
    ByteArrayInputStream xmlloadis = new ByteArrayInputStream(textString.getBytes("UTF-8"));
    try {
      this.unmarshaler.unmarshal(xmlloadis);
    } catch (XMLStreamException e) {
      throw new IOException(e);
    } catch (UnmarshallerException e) {
      throw new IOException(e);
    }
    XMLDocument doc = this.unmarshaler.getResult();
    doc.setNoNamespaceSchemaLocation(null);

    long after = System.currentTimeMillis();

    CoreDataObject root = (CoreDataObject) doc.getRootObject();
    MetricCollector visitor = new MetricCollector();
    root.accept(visitor);

    root.setValue(CloudGraphConstants.GRAPH_ASSEMBLY_TIME, Long.valueOf(after - before));
    root.setValue(CloudGraphConstants.GRAPH_NODE_COUNT, Long.valueOf(visitor.getCount()));
    root.setValue(CloudGraphConstants.GRAPH_DEPTH, Long.valueOf(visitor.getDepth()));

    Long time = (Long) root.getValue(CloudGraphConstants.GRAPH_ASSEMBLY_TIME);
    this.totalGraphUnmarshalTime = time.longValue();
    Long nodeCount = (Long) root.getValue(CloudGraphConstants.GRAPH_NODE_COUNT);
    this.totalGraphNodesAssembled = nodeCount.longValue();

    return doc.getRootObject().getDataGraph();
  }

}
