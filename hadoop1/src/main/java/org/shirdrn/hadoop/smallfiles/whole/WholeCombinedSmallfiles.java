package org.shirdrn.hadoop.smallfiles.whole;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.shirdrn.hadoop.smallfiles.IdentityReducer;

public class WholeCombinedSmallfiles {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: conbinesmallfiles <in> <out>");
	      System.exit(2);
	    }
	    
	    Job job = new Job(conf, "combine smallfiles");
	    
	    job.setJarByClass(WholeCombinedSmallfiles.class);
	    job.setMapperClass(WholeSmallfilesMapper.class);
	    job.setReducerClass(IdentityReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    job.setInputFormatClass(WholeFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    job.setNumReduceTasks(5);
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    
	    int exitFlag = job.waitForCompletion(true) ? 0 : 1;
	    System.exit(exitFlag);

	}

}
