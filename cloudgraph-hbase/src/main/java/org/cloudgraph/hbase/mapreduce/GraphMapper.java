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
package org.cloudgraph.hbase.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.cloudgraph.mapreduce.GraphMutator;
import org.cloudgraph.mapreduce.GraphWritable;

import commonj.sdo.DataGraph;

/**
 * Supplies fully realized data {@link GraphWritable graphs} as the input value
 * to MapReduce <code>Mapper</code> client subclasses, the input key being an
 * HBase row key <a href=
 * "http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/io/ImmutableBytesWritable.html"
 * >bytes</a> and the input value being a {@link GraphWritable} assembled from
 * one or more underlying HBase table(s)/row(s).
 * 
 * The data graphs supplied to the code>Mapper</code> are ready to modify but
 * the graph change summary must be set to track changes, so changes will be
 * detected. See the below code sample based on the Wikipedia domain model which
 * adds a fictitious category page link to each input graph.
 * <p>
 *
 * <pre>
 * public class PageGraphModifier
 * 		extends
 * 			GraphXmlMapper&lt;ImmutableBytesWritable, GraphWritable&gt; {
 * 	public void map(ImmutableBytesWritable offset, GraphWritable graph,
 * 			Context context) throws IOException {
 * 
 * 		// track changes
 * 		graph.getDataGraph().getChangeSummary().beginLogging();
 * 
 * 		Page page = (Page) graph.getDataGraph().getRootObject();
 * 		Categorylinks link = page.createCategorylinks();
 * 		link.setClTo(&quot;Some Category Page&quot;);
 * 		link.setClTimestamp((new Date()).toString());
 * 
 * 		// commit above changes
 * 		super.commit(row, graph, context);
 * 	}
 * }
 * </pre>
 *
 * </p>
 * 
 * <p>
 * Data graphs of any size of complexity may be supplied to MapReduce jobs
 * including graphs where the underlying domain model contains instances of
 * multiple inheritance. The set of data graphs is provided to a MapReduce job
 * using a <a
 * href="http://plasma-sdo.org/org/plasma/query/Query.html">query</a>, typically
 * supplied using {@link GraphMapReduceSetup}.
 * </p>
 * <p>
 * Data graphs are assembled within a {@link GraphRecordReader} based on the
 * detailed selection criteria within a given <a
 * href="http://plasma-sdo.org/org/plasma/query/Query.html">query</a>, and may
 * be passed to a {@link GraphRecordRecognizer} and potentially screened from
 * client {@link GraphMapper} extensions potentially illuminating business logic
 * dedicated to identifying specific records.
 * </p>
 * 
 * @param <KEYOUT>
 *            the output key type
 * @param <VALUEOUT>
 *            the output value type
 * 
 * @see org.cloudgraph.mapreduce.GraphWritable
 * @see org.cloudgraph.hbase.mapreduce.GraphRecordReader
 * @see org.cloudgraph.hbase.mapreduce.GraphMapReduceSetup
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public class GraphMapper<KEYOUT, VALUEOUT>
		extends
			Mapper<ImmutableBytesWritable, GraphWritable, KEYOUT, VALUEOUT>
		implements
			GraphMutator {

	private static Log log = LogFactory.getLog(GraphMapper.class);
	private GraphServiceDelegate serviceDelegate;

	/**
	 * Default constructor
	 */
	public GraphMapper() {
		this.serviceDelegate = new GraphServiceDelegate();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	 * org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(ImmutableBytesWritable row, GraphWritable graph,
			Context context) throws IOException {
		// no behavior
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cloudgraph.hbase.mapreduce.GraphMutator#commit(commonj.sdo.DataGraph,
	 * org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public void commit(DataGraph graph, JobContext jobContext)
			throws IOException {
		this.serviceDelegate.commit(graph, jobContext);
	}

	@Override
	public void commit(DataGraph[] graphs, JobContext jobContext)
			throws IOException {
		this.serviceDelegate.commit(graphs, jobContext);
	}
}
