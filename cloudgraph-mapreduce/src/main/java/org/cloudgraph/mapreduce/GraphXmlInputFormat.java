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
			TaskAttemptContext context) throws IOException,
			InterruptedException {
    	GraphXmlRecordReader reader = new GraphXmlRecordReader();
		try {
			reader.initialize(split, context);
		} catch (InterruptedException e) {
			throw new InterruptedIOException(e.getMessage());
		}
		
		return reader;
	}

}
