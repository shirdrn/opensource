package org.shirdrn.hadoop.extremum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ExtremumCostDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: extremumcost <in> <out>");
	      System.exit(2);
	    }
	    
	    Job job = new Job(conf, "extremum cost");
	    
	    job.setJarByClass(ExtremumCostDriver.class);
	    job.setMapperClass(ExtremunGlobalCostMapper.class);
	    job.setReducerClass(ExtremumGlobalCostReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Extremum.class);
	    job.setOutputFormatClass(ExtremumOutputFormat.class);
	    
	    job.setNumReduceTasks(2);
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    
	    int exitFlag = job.waitForCompletion(true) ? 0 : 1;
	    System.exit(exitFlag);
	}

}
