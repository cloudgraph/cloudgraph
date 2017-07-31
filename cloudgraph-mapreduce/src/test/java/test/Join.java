package test;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * INPUT A: User SSN --------------------------------------- sam 338-45-2339
 * fred 889-34-2334 mary 007-28-3994
 * 
 * INPUT B: User DOB --------------------------------------- mary 12/23/1998 sam
 * 04/23/2013 jim 09/12/2012
 */
public class Join {

	private static JoinType joinType = JoinType.inner;
	private static char VALUE_PREFIX_A = 'A';
	private static char VALUE_PREFIX_B = 'B';

	enum JoinType {
		inner, leftouter, rightouter, fullouter
	}

	public static class JoinMapperA
			extends
				Mapper<LongWritable, Text, Text, Text> {
		private static Log log = LogFactory.getLog(JoinMapperA.class);
		private Text outkey = new Text();
		private Text outvalue = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			log.info(value.toString());
			String[] values = value.toString().split("\\s+");
			outkey.set(values[0]); // assume join key is first value
			outvalue.set(VALUE_PREFIX_A + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class JoinMapperB
			extends
				Mapper<LongWritable, Text, Text, Text> {
		private static Log log = LogFactory.getLog(JoinMapperB.class);
		private Text outkey = new Text();
		private Text outvalue = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			log.info(value.toString());
			String[] values = value.toString().split("\\s+");
			outkey.set(values[0]); // assume join key is first value
			outvalue.set(VALUE_PREFIX_B + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		private static Log log = LogFactory.getLog(JoinReducer.class);
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String type = context.getConfiguration().get("jointype");
			if (type != null)
				joinType = JoinType.valueOf(type);
			listA.clear();
			listB.clear();
			for (Text text : values) {
				String value = text.toString();
				if (value.charAt(0) == VALUE_PREFIX_A) {
					listA.add(new Text(value.substring(1)));
				} else if (value.charAt(0) == VALUE_PREFIX_B) {
					listB.add(new Text(value.substring(1)));
				}
			}
			join(key, context);
		}

		private void join(Text key, Context context) throws IOException,
				InterruptedException {
			switch (joinType) {
				case inner :
					joinInner(key, context);
					break;
				case leftouter :
					joinLeftOuter(key, context);
					break;
				case rightouter :
					joinRightOuter(key, context);
					break;
				case fullouter :
					joinFullOuter(key, context);
					break;
				default :
					throw new IOException("invalid join type, " + joinType);
			}
		}

		void joinInner(Text key, Context context) throws IOException,
				InterruptedException {
			if (!listA.isEmpty() && !listB.isEmpty()) {
				for (Text a : listA) {
					for (Text b : listB) {
						context.write(a, b);
					}
				}
			}
		}

		void joinLeftOuter(Text key, Context context) throws IOException,
				InterruptedException {
			for (Text a : listA) {
				if (!listB.isEmpty()) { // If list B is not empty, join A and B
					for (Text b : listB) {
						context.write(a, b);
					}
				} else { // else, output A by itself
					context.write(a, new Text(" NULL NULL"));
				}
			}
		}

		void joinRightOuter(Text key, Context context) throws IOException,
				InterruptedException {
			for (Text b : listB) {

				if (!listA.isEmpty()) { // If list A is not empty, join A and B
					for (Text a : listA) {
						context.write(a, b);
					}
				} else { // else, output B by itself
					context.write(new Text("NULL NULL "), b);
				}
			}
		}

		void joinFullOuter(Text key, Context context) throws IOException,
				InterruptedException {
			if (!listA.isEmpty()) {
				// For each entry in A
				for (Text a : listA) {
					if (!listB.isEmpty()) { // If list B is not empty, join A
											// with B
						for (Text b : listB) {
							context.write(a, b);
						}
					} else {
						// Else, output A by itself
						context.write(a, new Text(" NULL NULL"));
					}
				}
			} else {
				// If list A is empty, just output B
				for (Text b : listB) {
					context.write(new Text("NULL NULL "), b);
				}
			}
		}
	}

	public static void runJob(Configuration conf, String[] args)
			throws IOException {
		Job job = new Job(conf);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, JoinMapperA.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, JoinMapperB.class);
		job.setReducerClass(JoinReducer.class);
		job.getConfiguration().set("jointype", args[2]);
		job.setNumReduceTasks(1);
		Path outPath = new Path("/tmp/jointest/"
				+ job.getConfiguration().get("jointype"));
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}

		try {
			job.waitForCompletion(true);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();

		// conf.set("mapred.child.java.opts",
		// "-Xms256m -Xmx2g -XX:+UseSerialGC");
		// conf.set("mapred.job.map.memory.mb", "4096");
		// conf.set("mapred.job.reduce.memory.mb", "1024");

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length == 0) {
			System.out.println("Wrong number of parameters: " + args.length);
			System.exit(-1);
		}
		try {
			runJob(conf, otherArgs);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

}
