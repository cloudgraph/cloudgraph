package test;

import java.io.IOException;

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
 * INPUT: Unique-Id Space-Seperated-Keywords
 * --------------------------------------- 12718 Ferrari Audi Alpina 93729
 * Alpina Chrysler 92837 BMW Acura Audi 09283 Audi Cadillac 97283 Acura Cadillac
 * Alpina 92837 BMW Alpina 19203 Cadillac Buick BMW
 * 
 * OUTPUT: Keyword Space-Seperated-Unique-Ids
 * --------------------------------------- Acura 97283 92837 Alpina 97283 92837
 * 12718 93729 Audi 92837 09283 12718 BMW 92837 19203 92837 Buick 19203 Cadillac
 * 09283 97283 19203 Chrysler 93729 Ferrari 12718
 */
public class InvertedIndex {

  public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text id = new Text();
    private Text outkey = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String[] values = value.toString().split("\\s+");
      id.set(values[0]);
      for (int i = 1; i < values.length; i++) {
        outkey.set(values[i]);
        context.write(outkey, id);
      }
    }
  }

  public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Text id : values) {
        if (first) {
          first = false;
        } else {
          sb.append(" ");
        }
        sb.append(id.toString());
      }
      result.set(sb.toString());
      context.write(key, result);
    }
  }

  public static void runJob(Configuration conf, String[] args) throws IOException {
    Job job = new Job(conf);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
        InvertedIndexMapper.class);
    job.setReducerClass(InvertedIndexReducer.class);
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
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 1) {
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
