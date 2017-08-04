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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.plasma.sdo.helper.PlasmaXMLHelper;
import org.plasma.sdo.xml.DefaultOptions;
import org.plasma.sdo.xml.StreamMarshaller;
import org.plasma.sdo.xml.StreamUnmarshaller;

import commonj.sdo.DataGraph;
import commonj.sdo.helper.XMLDocument;

/**
 * 
 * @author Scott Cinnamond
 * @since 0.6.0
 */
public class GraphXmlRecordWriter extends RecordWriter<LongWritable, GraphWritable> {
  static final Log log = LogFactory.getLog(GraphXmlRecordWriter.class);
  private static final String utf8 = "UTF-8";
  private static final byte[] newline;
  static {
    try {
      newline = "\n".getBytes(utf8);
    } catch (UnsupportedEncodingException uee) {
      throw new IllegalArgumentException("can't find " + utf8 + " encoding");
    }
  }
  protected DataOutputStream out;
  private String rootNamespaceUri;
  private String rootNamespacePrefix;
  private DefaultOptions marshalOptions;
  private Method getCounter = null;
  private TaskAttemptContext context;
  private Configuration configuration;
  private long totalGraphMarshalTime;

  public GraphXmlRecordWriter(DataOutputStream out, TaskAttemptContext job) {
    this.context = job;
    this.configuration = job.getConfiguration();
    this.out = out;
    try {
      this.getCounter = Counters.retrieveGetCounterWithStringsParams(job);
    } catch (IOException e) {
    }

    this.rootNamespaceUri = configuration.get(GraphXmlInputFormat.ROOT_ELEM_NAMESPACE_URI);
    this.rootNamespacePrefix = configuration.get(GraphXmlInputFormat.ROOT_ELEM_NAMESPACE_PREFIX,
        "ns1");

    this.marshalOptions = new DefaultOptions(this.rootNamespaceUri);
    this.marshalOptions.setRootNamespacePrefix(this.rootNamespacePrefix);
    this.marshalOptions.setValidate(false);
    this.marshalOptions.setFailOnValidationError(false);
  }

  @Override
  public void close(TaskAttemptContext job) throws IOException, InterruptedException {
    out.close();
  }

  @Override
  public void write(LongWritable arg0, GraphWritable graph) throws IOException,
      InterruptedException {

    out.write(serializeGraph(graph.getDataGraph()));
    out.write(newline);

    updateCounters();
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
          Counters.CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_XML_MARSHAL_TIME))
          .increment(this.totalGraphMarshalTime);

    } catch (Exception e) {
      log.debug("can't update counter." + StringUtils.stringifyException(e));
    }
  }

  private byte[] serializeGraph(DataGraph graph) throws IOException {
    long before = System.currentTimeMillis();
    DefaultOptions options = new DefaultOptions(graph.getRootObject().getType().getURI());
    options.setRootNamespacePrefix(this.rootNamespacePrefix);
    options.setPrettyPrint(false);
    XMLDocument doc = PlasmaXMLHelper.INSTANCE.createDocument(graph.getRootObject(), graph
        .getRootObject().getType().getURI(), null);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PlasmaXMLHelper.INSTANCE.save(doc, os, options);
    os.flush();
    long after = System.currentTimeMillis();

    this.totalGraphMarshalTime = after - before;
    return os.toByteArray();
  }

}
