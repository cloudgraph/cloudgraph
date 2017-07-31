package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.cloudgraph.common.CommonTest;
import org.cloudgraph.common.CommonTestSetup;

import junit.framework.Test;

public class InvertedIndexTest extends CommonTest {

	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	public static Test suite() {
		return CommonTestSetup.newTestSetup(InvertedIndexTest.class);
	}

	public void setUp() {
		InvertedIndex.InvertedIndexMapper mapper = new InvertedIndex.InvertedIndexMapper();
		InvertedIndex.InvertedIndexReducer reducer = new InvertedIndex.InvertedIndexReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("12718 Ferrari"));
		mapDriver.addOutput(new Text("Ferrari"), new Text("12718"));
		mapDriver.runTest();
	}

	public void testReducer() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("12716"));
		values.add(new Text("12717"));
		values.add(new Text("12718"));
		reduceDriver.withInput(new Text("Ferrari"), values);
		reduceDriver.addOutput(new Text("Ferrari"), new Text(
				"12716 12717 12718"));
		// reduceDriver.with
		reduceDriver.runTest();
	}

	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text(
				"12718 Ferrari Audi BMW"));
		mapReduceDriver.addOutput(new Text("Audi"), new Text("12718"));
		mapReduceDriver.addOutput(new Text("BMW"), new Text("12718"));
		mapReduceDriver.addOutput(new Text("Ferrari"), new Text("12718"));
		mapReduceDriver.runTest();
	}
}
