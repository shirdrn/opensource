package org.shirdrn.hadoop.extremum.max;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GlobalMaxCostDriver {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		 
			Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    if (otherArgs.length != 2) {
		      System.err.println("Usage: maxcost <in> <out>");
		      System.exit(2);
		    }
		    
		    Job job = new Job(conf, "max cost");
		    
		    job.setJarByClass(GlobalMaxCostDriver.class);
		    job.setMapperClass(GlobalCostMapper.class);
		    job.setCombinerClass(GlobalCostReducer.class);
		    job.setReducerClass(GlobalCostReducer.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    
		    int exitFlag = job.waitForCompletion(true) ? 0 : 1;
		    System.exit(exitFlag);

	}

}
