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

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class Counters {
  /** Name of mapreduce counter group for CloudGraph */
  public static final String CLOUDGRAPH_COUNTER_GROUP_NAME = "CloudGraph Counters";

  /**
   * MapReduce counter which stores the number of data graphs successfully
   * recognized by the current recognizer. Will remain zero if no recognizer is
   * required for the current query
   */
  public static final String CLOUDGRAPH_COUNTER_NAME_NUM_RECOGNIZED_GRAPHS = "NUM_RECOGNIZED_GRAPHS";

  /**
   * MapReduce counter which stores the number of data graphs not recognized by
   * the current recognizer. Will remain zero if no recognizer is required for
   * the current query
   */
  public static final String CLOUDGRAPH_COUNTER_NAME_NUM_UNRECOGNIZED_GRAPHS = "NUM_UNRECOGNIZED_GRAPHS";

  /** MapReduce counter which stores the total number of graph nodes assembled */
  public static final String CLOUDGRAPH_COUNTER_NAME_NUM_GRAPH_NODES_ASSEMBLED = "NUM_GRAPH_NODES_ASSEMBLED";

  /**
   * MapReduce counter which stores the total time in milliseconds taken for
   * graph assembly. Note: this time counter is summed across all tasks on all
   * hosts so could exceed the total time taken for the job, as the tasks of
   * course run in parallel.
   */
  public static final String CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_ASSEMBLY_TIME = "MILLIS_GRAPH_ASSEMBLY";

  /**
   * MapReduce counter which stores the total time in milliseconds taken for
   * graph recognition
   */
  public static final String CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_RECOG_TIME = "MILLIS_GRAPH_RECOGNITION";

  /**
   * MapReduce counter which stores the total time in milliseconds taken for
   * graph XMl unmarshalling. Note: this time counter is summed across all tasks
   * on all hosts so could exceed the total time taken for the job, as the tasks
   * of course run in parallel.
   */
  public static final String CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_XML_UNMARSHAL_TIME = "MILLIS_GRAPH_XML_UNMARSHAL";

  /**
   * MapReduce counter which stores the total time in milliseconds taken for
   * graph XMl marshalling. Note: this time counter is summed across all tasks
   * on all hosts so could exceed the total time taken for the job, as the tasks
   * of course run in parallel.
   */
  public static final String CLOUDGRAPH_COUNTER_NAME_TOT_GRAPH_XML_MARSHAL_TIME = "MILLIS_GRAPH_XML_MARSHAL";

  /**
   * In new mapreduce APIs, TaskAttemptContext has two getCounter methods Check
   * if getCounter(String, String) method is available.
   * 
   * @return The getCounter method or null if not available.
   * @throws IOException
   */
  public static Method retrieveGetCounterWithStringsParams(TaskAttemptContext context)
      throws IOException {
    Method m = null;
    try {
      m = context.getClass().getMethod("getCounter", new Class[] { String.class, String.class });
    } catch (SecurityException e) {
      throw new IOException("Failed test for getCounter", e);
    } catch (NoSuchMethodException e) {
      // Ignore
    }
    return m;
  }

}
