/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
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
public class GraphXmlRecordWriter
		extends
			RecordWriter<LongWritable, GraphWritable> {
	static final Log log = LogFactory.getLog(GraphXmlRecordWriter.class);
	private static final String utf8 = "UTF-8";
	private static final byte[] newline;
	static {
		try {
			newline = "\n".getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
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

		this.rootNamespaceUri = configuration
				.get(GraphXmlInputFormat.ROOT_ELEM_NAMESPACE_URI);
		this.rootNamespacePrefix = configuration.get(
				GraphXmlInputFormat.ROOT_ELEM_NAMESPACE_PREFIX, "ns1");

		this.marshalOptions = new DefaultOptions(this.rootNamespaceUri);
		this.marshalOptions.setRootNamespacePrefix(this.rootNamespacePrefix);
		this.marshalOptions.setValidate(false);
		this.marshalOptions.setFailOnValidationError(false);
	}

	@Override
	public void close(TaskAttemptContext job) throws IOException,
			InterruptedException {
		out.close();
	}

	@Override
	public void write(LongWritable arg0, GraphWritable graph)
			throws IOException, InterruptedException {

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

			((Counter) this.getCounter
					.invoke(context,
							Counters.CLOUDGRAPH_COUNTER_GROUP_NAME,
							Counters.CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_XML_MARSHAL_TIME))
					.increment(this.totalGraphMarshalTime);

		} catch (Exception e) {
			log.debug("can't update counter."
					+ StringUtils.stringifyException(e));
		}
	}

	private byte[] serializeGraph(DataGraph graph) throws IOException {
		long before = System.currentTimeMillis();
		DefaultOptions options = new DefaultOptions(graph.getRootObject()
				.getType().getURI());
		options.setRootNamespacePrefix(this.rootNamespacePrefix);
		options.setPrettyPrint(false);
		XMLDocument doc = PlasmaXMLHelper.INSTANCE.createDocument(
				graph.getRootObject(),
				graph.getRootObject().getType().getURI(), null);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		PlasmaXMLHelper.INSTANCE.save(doc, os, options);
		os.flush();
		long after = System.currentTimeMillis();

		this.totalGraphMarshalTime = after - before;
		return os.toByteArray();
	}

}
