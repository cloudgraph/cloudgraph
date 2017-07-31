package test;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * INPUT: Make Rating (1-100) --------------------------------------- Acura 47
 * Alpina 99 Arrinera 57 Artega 41 Ascari 27 Audi 44 BMW 93 Bentley 94 Bertone
 * 33 Brabus 35 Breckland 73 Bugatti 60 Buick 85 ...
 * 
 * OUTPUT (Top N): Make Rating (1-100) ---------------------------------------
 * Kia 100 Opel 99 Vauxhall 98 Tramontana 97 Tesla 96 Mazda 95 Mindset 94 ORCA
 * 93 Stola 92 Saturn 91
 * 
 */
public class Filtering {

	public static class FilteringMapper
			extends
				Mapper<Object, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> map = new TreeMap<Integer, Text>();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] parsed = value.toString().split("\\s+");
			map.put(Integer.parseInt(parsed[1]), new Text(value));
			if (map.size() > 10) {
				map.remove(map.firstKey());
			}
		}
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : map.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static class FilteringReducer
			extends
				Reducer<NullWritable, Text, NullWritable, Text> {
		private TreeMap<Integer, Text> map = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				String[] parsed = value.toString().split("\\s+");
				map.put(Integer.parseInt(parsed[1]), new Text(value));
				if (map.size() > 10) {
					map.remove(map.firstKey());
				}
			}
			for (Text t : map.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void runJob(Configuration conf, String[] args)
			throws IOException {
		Job job = new Job(conf);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, FilteringMapper.class);
		job.setReducerClass(FilteringReducer.class);
		job.setNumReduceTasks(1);
		Path outPath = new Path("/tmp/test");
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
