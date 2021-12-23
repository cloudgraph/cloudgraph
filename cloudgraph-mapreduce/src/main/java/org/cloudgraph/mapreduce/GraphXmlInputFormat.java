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
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * An XML oriented input format based on <code>FileInputFormat</code>
 * 
 * @see org.cloudgraph.mapreduce.GraphXmlRecordReader
 * @see org.cloudgraph.mapreduce.GraphWritable
 * 
 * @author Scott Cinnamond
 * @since 0.5.8
 */
public class GraphXmlInputFormat extends FileInputFormat<LongWritable, GraphWritable> implements
    Configurable, GraphXml {

  /** The configuration. */
  private Configuration conf = null;

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public RecordReader<LongWritable, GraphWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    GraphXmlRecordReader reader = new GraphXmlRecordReader();
    try {
      reader.initialize(split, context);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }

    return reader;
  }

}
