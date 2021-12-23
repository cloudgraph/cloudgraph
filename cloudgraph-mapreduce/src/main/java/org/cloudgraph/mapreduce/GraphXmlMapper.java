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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Supplies fully realized data {@link GraphWritable graphs} as the input value
 * to MapReduce <code>Mapper</code> client subclasses, the input key being an
 * offset into the processed file and the value being a {@link GraphWritable}
 * assembled from a single SDO graph XML line read from the file. Supports
 * detection of changes to the input data graph, and propagation of mutations to
 * the underlying data store.
 * 
 * The data graphs supplied to the code>Mapper</code> are ready to further
 * modify or simply commit as is, for <code>Mapper</code> clients wishing to
 * modify input graphs and commit changes within the map phase. See the below
 * code sample based on the Wikipedia domain model which adds a page link to
 * each input graph.
 * <p>
 *
 * <pre>
 * public class PageGraphImporter extends GraphXmlMapper&lt;LongWritable, GraphWritable&gt; {
 *   &#064;Override
 *   public void map(LongWritable offset, GraphWritable graph, Context context) throws IOException {
 * 
 *     Page page = (Page) graph.getDataGraph().getRootObject();
 *     page.setPageTitle(&quot;New Page1&quot;);
 * 
 *     // commit above changes
 *     super.commit(row, graph, context);
 *   }
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
 * Data graphs are assembled within a {@link GraphXmlRecordReader} based on the
 * line oriented XML graph data read from an underlying file, and are passed to
 * client {@link GraphXmlMapper} extensions.
 * </p>
 * 
 * @param <KEYOUT>
 *          the output key type
 * @param <VALUEOUT>
 *          the output value type
 * 
 * @see org.cloudgraph.mapreduce.GraphWritable
 * @see org.cloudgraph.mapreduce.GraphXmlRecordReader
 * @see org.cloudgraph.hbase.mapreduce.GraphMapReduceSetup
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public class GraphXmlMapper<KEYOUT, VALUEOUT> extends
    Mapper<LongWritable, GraphWritable, KEYOUT, VALUEOUT> {

  private static Log log = LogFactory.getLog(GraphXmlMapper.class);

  public GraphXmlMapper() {
  }

  @Override
  public void map(LongWritable row, GraphWritable graph, Context context) throws IOException {
    // no behavior
  }

}
