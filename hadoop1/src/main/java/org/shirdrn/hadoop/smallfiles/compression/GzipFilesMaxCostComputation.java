package org.shirdrn.hadoop.smallfiles.compression;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GzipFilesMaxCostComputation {

	public static class GzipFilesMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private final static LongWritable costValue = new LongWritable(0);
	    private Text code = new Text();
	    
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// a line, such as 'SG 253654006139495 253654006164392 619850464'
			String line = value.toString();
			String[] array = line.split("\\s");
			if(array.length == 4) {
				String countryCode = array[0];
				String strCost = array[3];
				long cost = 0L;
				try {
					cost = Long.parseLong(strCost);
				} catch (NumberFormatException e) {
					cost = 0L;
				}
				if(cost != 0) {
					code.set(countryCode);
					costValue.set(cost);
					context.write(code, costValue);
				}
			}
		}
	}
	
	public static class GzipFilesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long max = 0L;
			Iterator<LongWritable> iter = values.iterator();
			while(iter.hasNext()) {
				LongWritable current = iter.next();
				if(current.get() > max) {
					max = current.get();
				}
			}
			context.write(key, new LongWritable(max));
		}
		
	}
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: gzipmaxcost <in> <out>");
	      System.exit(2);
	    }
	    
	    Job job = new Job(conf, "gzip maxcost");
	    
	    job.getConfiguration().setBoolean("mapred.output.compress", true);
	    job.getConfiguration().setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
	    
	    job.setJarByClass(GzipFilesMaxCostComputation.class);
	    job.setMapperClass(GzipFilesMapper.class);
	    job.setCombinerClass(GzipFilesReducer.class);
	    job.setReducerClass(GzipFilesReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    
	    job.setNumReduceTasks(1);
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    
	    int exitFlag = job.waitForCompletion(true) ? 0 : 1;
	    System.exit(exitFlag);

	}
}
