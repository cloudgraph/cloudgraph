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

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class Counters {
	/** Name of mapreduce counter group for CloudGraph */
	public static final String CLOUDGRAPH_COUNTER_GROUP_NAME = "CloudGraph Counters";
	
	/** 
	 * MapReduce counter which stores the number of data graphs successfully 
	 * recognized by the current recognizer. Will remain zero if no recognizer
	 * is required for the current query */
	public static final String CLOUDGRAPH_COUNTER_NAME_NUM_RECOGNIZED_GRAPHS = "NUM_RECOGNIZED_GRAPHS";
	
	/** 
	 * MapReduce counter which stores the number of data graphs not recognized by the 
	 * current recognizer. Will remain zero if no recognizer
	 * is required for the current query */
	public static final String CLOUDGRAPH_COUNTER_NAME_NUM_UNRECOGNIZED_GRAPHS = "NUM_UNRECOGNIZED_GRAPHS";
	
	/** MapReduce counter which stores the total number of graph nodes assembled */
	public static final String CLOUDGRAPH_COUNTER_NAME_NUM_GRAPH_NODES_ASSEMBLED = "NUM_GRAPH_NODES_ASSEMBLED";
	
	/** MapReduce counter which stores the total time in milliseconds taken for graph assembly. Note: this
	 * time counter is summed across all tasks on all hosts so could exceed the total time taken for
	 * the job, as the tasks of course run in parallel.  */
	public static final String CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_ASSEMBLY_TIME = "MILLIS_GRAPH_ASSEMBLY";	

	/** MapReduce counter which stores the total time in milliseconds taken for graph recognition */
	public static final String CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_RECOG_TIME = "MILLIS_GRAPH_RECOGNITION";	

	/** MapReduce counter which stores the total time in milliseconds taken for graph XMl unmarshalling. Note: this
	 * time counter is summed across all tasks on all hosts so could exceed the total time taken for
	 * the job, as the tasks of course run in parallel. */
	public static final String CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_XML_UNMARSHAL_TIME = "MILLIS_GRAPH_XML_UNMARSHAL";
	
	/** MapReduce counter which stores the total time in milliseconds taken for graph XMl marshalling. Note: this
	 * time counter is summed across all tasks on all hosts so could exceed the total time taken for
	 * the job, as the tasks of course run in parallel. */
	public static final String CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_XML_MARSHAL_TIME = "MILLIS_GRAPH_XML_MARSHAL";
	

	/**
	 * In new mapreduce APIs, TaskAttemptContext has two getCounter methods
	 * Check if getCounter(String, String) method is available.
	 * 
	 * @return The getCounter method or null if not available.
	 * @throws IOException
	 */
	public static Method retrieveGetCounterWithStringsParams(
			TaskAttemptContext context) throws IOException {
		Method m = null;
		try {
			m = context.getClass().getMethod("getCounter",
					new Class[] { String.class, String.class });
		} catch (SecurityException e) {
			throw new IOException("Failed test for getCounter", e);
		} catch (NoSuchMethodException e) {
			// Ignore
		}
		return m;
	}

}
