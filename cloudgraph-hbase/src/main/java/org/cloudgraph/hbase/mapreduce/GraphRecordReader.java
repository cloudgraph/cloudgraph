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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.cloudgraph.core.client.Scan;
//import org.cloudgraph.core.client.Table;
import org.cloudgraph.mapreduce.GraphWritable;

/**
 * Iterates over HBase root table data for the current <code>TableSplit</code>,
 * assembling data graphs based on the detailed selection criteria within a
 * given <a href="http://plasma-sdo.org/org/plasma/query/Query.html">query</a>.
 * Partially or fully assembled data graphs may be passed to a
 * {@link GraphRecordRecognizer} and potentially screened from client
 * {@link GraphMapper} extensions potentially illuminating business logic
 * dedicated to identifying specific records.
 */
public class GraphRecordReader extends RecordReader<ImmutableBytesWritable, GraphWritable> {

  private GraphRecordRecognizer recordReaderImpl;
  private Configuration configuration;

  @SuppressWarnings("unused")
  private GraphRecordReader() {
  }

  public GraphRecordReader(Configuration configuration) {
    this.configuration = configuration;
    recordReaderImpl = new GraphRecordRecognizer(configuration);
  }

  /**
   * Restart from survivable exceptions by creating a new scanner.
   * 
   * @param firstRow
   *          The first row to start at.
   * @throws IOException
   *           When restarting fails.
   */
  public void restart(byte[] firstRow) throws IOException {
    this.recordReaderImpl.restart(firstRow);
  }

  /**
   * Sets the Table.
   * 
   * @param table
   *          The {@link Table} to scan.
   */
  public void setTable(Table table) {
    this.recordReaderImpl.setTable(table);
  }

  /**
   * Sets the scan defining the actual details like columns etc.
   * 
   * @param scan
   *          The scan to set.
   */
  public void setScan(Scan scan) {
    this.recordReaderImpl.setScan(scan);
  }

  /**
   * Closes the split.
   * 
   * @see org.apache.hadoop.mapreduce.RecordReader#close()
   */
  @Override
  public void close() {
    this.recordReaderImpl.close();
  }

  /**
   * Returns the current key.
   * 
   * @return The current key.
   * @throws IOException
   * @throws InterruptedException
   *           When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
   */
  @Override
  public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
    return this.recordReaderImpl.getCurrentKey();
  }

  /**
   * Returns the current value.
   * 
   * @return The current value.
   * @throws IOException
   *           When the value is faulty.
   * @throws InterruptedException
   *           When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
   */
  @Override
  public GraphWritable getCurrentValue() throws IOException, InterruptedException {
    return this.recordReaderImpl.getCurrentValue();
  }

  /**
   * Initializes the reader.
   * 
   * @param inputsplit
   *          The split to work with.
   * @param context
   *          The current task context.
   * @throws IOException
   *           When setting up the reader fails.
   * @throws InterruptedException
   *           When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit,
   *      org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException,
      InterruptedException {
    this.recordReaderImpl.initialize(inputsplit, context);
  }

  /**
   * Positions the record reader to the next record.
   * 
   * @return <code>true</code> if there was another record.
   * @throws IOException
   *           When reading the record failed.
   * @throws InterruptedException
   *           When the job was aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return this.recordReaderImpl.nextKeyValue();
  }

  /**
   * The current progress of the record reader through its data.
   * 
   * @return A number between 0.0 and 1.0, the fraction of the data read.
   * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
   */
  @Override
  public float getProgress() {
    return this.recordReaderImpl.getProgress();
  }
}
